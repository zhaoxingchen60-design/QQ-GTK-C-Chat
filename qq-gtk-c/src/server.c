/*
 * server.c — QQ 聊天系统服务端实现
 *
 * ======== 架构概览 ========
 *
 * 服务端采用"主线程 accept + 线程池处理"的并发模型：
 *
 *   main() 线程:
 *     └─ 循环 accept() 新连接 → 封装为 Job → 压入全局任务队列 g_queue
 *
 *   Worker 线程 (默认 8 个):
 *     └─ 循环从 g_queue 取 Job → handle_client() 处理客户端完整生命周期
 *
 * 这种设计的好处：
 *   1. accept 不会被慢客户端阻塞（worker 线程并发处理）
 *   2. 线程池大小固定，避免无限创建线程导致资源耗尽
 *   3. 每个客户端连接由一个 worker 线程全程服务，逻辑简单
 *
 * ======== 数据持久化 ========
 *
 * 所有数据以 TSV 文件存储在 data/ 目录下：
 *   users.tsv           — 用户账号/显示名/密码
 *   friends.tsv         — 好友关系（A→B 表示 A 添加了 B）
 *   friend_requests.tsv — 待处理的好友申请
 *   groups.tsv          — 群成员关系
 *   group_invites.tsv   — 待处理的群邀请
 *   messages.tsv        — 历史消息（全局，按时间顺序追加）
 *   offline.tsv         — 离线消息（用户上线后投递并删除）
 *
 * 每条记录用 TAB 分隔字段，换行分隔记录，与网络协议格式保持一致。
 * 文件读写均用 g_store_mutex 互斥锁保护，防止并发写导致数据损坏。
 *
 * ======== 线程安全设计 ========
 *
 * 三个核心锁：
 *   g_queue.mutex      — 任务队列锁（生产者-消费者模型）
 *   g_clients_mutex    — 在线客户端表锁（保护 g_clients[] 数组）
 *   g_store_mutex      — 数据文件锁（保护所有 TSV 文件读写）
 *
 * 每个在线客户端还有一个 send_mutex：
 *   防止多个线程同时给同一个客户端发送消息时串包
 *   （例如 worker-A 发好友列表，worker-B 同时转发群消息）
 *
 * ======== 共享内存统计 ========
 *
 * 使用 POSIX 共享内存 /qq_gtk_c_stats 存储全局统计信息。
 * 这不是架构必需的，纯粹是为了在课程设计中展示"进程共享内存"的使用。
 * 多个服务器进程可以通过此共享内存共享统计数据。
 *
 * ======== 协议命令列表 ========
 *
 * 客户端 → 服务端：
 *   LOGIN / REGISTER  — 登录/注册（必须是第一条命令）
 *   QUIT              — 断开连接
 *   LIST              — 刷新好友列表和群列表
 *   ADD_FRIEND        — 发送好友申请
 *   ACCEPT_FRIEND     — 同意好友申请
 *   REJECT_FRIEND     — 拒绝好友申请
 *   DELETE_FRIEND     — 删除好友
 *   MSGP              — 发送私聊消息
 *   MSGG              — 发送群聊消息
 *   CREATE_GROUP      — 创建群
 *   INVITE_GROUP      — 邀请好友入群
 *   ACCEPT_GROUP      — 同意群邀请
 *   REJECT_GROUP      — 拒绝群邀请
 *   LEAVE_GROUP       — 退出群
 *   HISTORY           — 查询历史消息
 *   SEARCH            — 检索历史消息
 *   PEER_INFO         — 查询对端 P2P 地址
 *   FILE_NOTICE       — 文件传输通知
 *
 * 服务端 → 客户端：
 *   OK / ERR          — 操作结果
 *   MSG / OFFLINE     — 聊天消息 / 离线消息
 *   FRIEND / FRIENDS_BEGIN / FRIENDS_END — 好友列表
 *   GROUP / GROUPS_BEGIN / GROUPS_END    — 群列表
 *   COUNTS            — 好友数量统计
 *   FRIEND_REQUEST    — 好友申请通知
 *   GROUP_INVITE      — 群邀请通知
 *   HISTORY / HISTORY_BEGIN / HISTORY_END — 历史消息
 *   SEARCH / SEARCH_BEGIN / SEARCH_END   — 检索结果
 *   PEER              — P2P 对端信息
 *   NOTICE            — 系统通知
 *   BYE               — 服务端主动断开
 *   ONLINE            — 在线人数变化
 */

#include "protocol.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* ======== 全局常量 ======== */

/* 最大同时在线客户端数 */
#define MAX_CLIENTS 256

/* 任务队列最大容量（accept 线程与 worker 线程之间的缓冲） */
#define MAX_QUEUE 512

/* 默认 worker 线程数（可通过命令行参数修改） */
#define DEFAULT_WORKERS 8

/* 持久化数据存储目录 */
#define DATA_DIR "data"

/* 共享内存名称（用于进程间共享统计数据） */
#define SHM_NAME "/qq_gtk_c_stats"

/* ======== 数据结构定义 ======== */

/*
 * Job — 任务队列中的一个工作单元
 *
 * accept 线程将新连接的 fd 和客户端 IP 封装为 Job，
 * 压入全局任务队列，等待 worker 线程取出处理。
 */
typedef struct {
    int fd;                          /* 客户端 socket 文件描述符 */
    char ip[INET_ADDRSTRLEN];        /* 客户端 IP 地址（点分十进制） */
} Job;

/*
 * JobQueue — 线程安全的环形任务队列
 *
 * 生产者（accept 线程）通过 queue_push() 在队尾添加任务。
 * 消费者（worker 线程）通过 queue_pop() 从队首取出任务。
 *
 * 使用 mutex + 条件变量的经典生产者-消费者模式：
 *   - 队列满时生产者阻塞等待（has_job 条件变量同时用于有空位和有任务两种通知）
 *   - 队列空时消费者阻塞等待
 */
typedef struct {
    Job items[MAX_QUEUE];            /* 环形队列存储 */
    int head;                        /* 队首索引（消费者下次取的位置） */
    int tail;                        /* 队尾索引（生产者下次放的位置） */
    int count;                       /* 当前队列中的任务数 */
    pthread_mutex_t mutex;           /* 保护队列结构的互斥锁 */
    pthread_cond_t has_job;          /* 条件变量：队列非空或有空位 */
} JobQueue;

/*
 * Client — 在线客户端状态
 *
 * 每个已登录的客户端在 g_clients[] 数组中占用一个槽位。
 * send_mutex 保证多线程给同一客户端发送时消息不会串包。
 *
 * active 标记用于槽位复用：客户端断开后 active=0，槽位可被新连接使用。
 */
typedef struct {
    int fd;                          /* 该客户端的 socket 连接 */
    int active;                      /* 槽位是否被占用（1=在线，0=空闲） */
    int peer_port;                   /* 该客户端 P2P 文件传输监听端口 */
    char user[QQ_MAX_NAME];         /* 用户名（登录标识） */
    char display[QQ_MAX_NAME];      /* 显示名（昵称） */
    char ip[INET_ADDRSTRLEN];       /* 客户端 IP 地址 */
    pthread_mutex_t send_mutex;      /* 发送锁：防止并发发送串包 */
} Client;

/*
 * SharedStats — 通过共享内存跨进程访问的统计数据
 *
 * 使用 POSIX 共享内存 + 进程共享的互斥锁，
 * 允许多个服务端进程共享同一份统计数据。
 *
 * 这是课程设计中"进程间通信（IPC）"部分的演示。
 */
typedef struct {
    pthread_mutex_t mutex;           /* 进程共享的互斥锁 */
    int initialized;                 /* 是否已初始化（0=未初始化） */
    int online_users;                /* 当前在线用户数 */
    int total_logins;                /* 历史总登录次数 */
    long private_messages;           /* 私聊消息总数 */
    long group_messages;             /* 群聊消息总数 */
} SharedStats;

/* ======== 全局变量 ======== */

/* 服务端运行标志，收到 SIGINT/SIGTERM 时设为 0，触发优雅退出 */
static volatile sig_atomic_t g_running = 1;

/* 全局任务队列（accept 线程与 worker 线程之间的桥梁） */
static JobQueue g_queue = {
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .has_job = PTHREAD_COND_INITIALIZER
};

/* 在线客户端表，用 g_clients_mutex 保护 */
static Client g_clients[MAX_CLIENTS];

/* 在线客户端表互斥锁 */
static pthread_mutex_t g_clients_mutex = PTHREAD_MUTEX_INITIALIZER;

/* 数据文件互斥锁（保护所有 TSV 文件的读写） */
static pthread_mutex_t g_store_mutex = PTHREAD_MUTEX_INITIALIZER;

/* 共享内存统计数据指针 */
static SharedStats *g_stats = NULL;

/* ======== 工具函数 ======== */

/* 打印错误信息并退出程序（仅用于不可恢复的启动错误） */
static void die(const char *msg) {
    perror(msg);
    exit(1);
}

/* 信号处理函数：收到 SIGINT 或 SIGTERM 时设置退出标志 */
static void on_signal(int sig) {
    (void)sig;
    g_running = 0;  /* 主循环和 worker 线程检测到此标志后退出 */
}

/* 确保数据目录存在，不存在则创建 */
static void ensure_storage(void) {
    mkdir(DATA_DIR, 0755);
}

/*
 * init_shared_stats — 初始化 POSIX 共享内存统计数据
 *
 * 步骤：
 *   1. 创建/打开命名共享内存 /qq_gtk_c_stats
 *   2. 设置共享内存大小为 SharedStats 结构体大小
 *   3. mmap 映射到进程地址空间
 *   4. 首次创建时初始化互斥锁（设为进程共享属性）和统计数据
 *
 * 使用 PTHREAD_PROCESS_SHARED 属性的互斥锁可以跨进程使用，
 * 这是 pthread 的高级特性之一。
 */
static void init_shared_stats(void) {
    /* shm_open: 创建或打开命名共享内存对象 */
    int fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0600);
    if (fd < 0) {
        perror("shm_open");
        return;
    }
    /* 设置共享内存对象的大小 */
    if (ftruncate(fd, (off_t)sizeof(SharedStats)) < 0) {
        perror("ftruncate");
        close(fd);
        return;
    }
    /* 映射到进程地址空间，MAP_SHARED 使得修改对所有映射了此区域的进程可见 */
    g_stats = mmap(NULL, sizeof(SharedStats), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);  /* mmap 后可以关闭文件描述符 */
    if (g_stats == MAP_FAILED) {
        perror("mmap");
        g_stats = NULL;
        return;
    }
    /* 首次初始化：设置进程共享互斥锁属性 */
    if (!g_stats->initialized) {
        pthread_mutexattr_t attr;
        pthread_mutexattr_init(&attr);
        /* PTHREAD_PROCESS_SHARED 是关键：允许多个进程共享此锁 */
        pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
        pthread_mutex_init(&g_stats->mutex, &attr);
        pthread_mutexattr_destroy(&attr);
        g_stats->online_users = 0;
        g_stats->total_logins = 0;
        g_stats->private_messages = 0;
        g_stats->group_messages = 0;
        g_stats->initialized = 1;  /* 标记已初始化 */
    }
}

/*
 * ======== 统计更新函数（共享内存 + 锁保护） ========
 */

/* 登录时更新：总登录次数 + 在线人数 */
static void stats_login(int online_users) {
    if (!g_stats) {
        return;
    }
    pthread_mutex_lock(&g_stats->mutex);
    g_stats->total_logins++;
    g_stats->online_users = online_users;
    pthread_mutex_unlock(&g_stats->mutex);
}

/* 在线人数变化时更新 */
static void stats_online(int online_users) {
    if (!g_stats) {
        return;
    }
    pthread_mutex_lock(&g_stats->mutex);
    g_stats->online_users = online_users;
    pthread_mutex_unlock(&g_stats->mutex);
}

/* 消息发送时更新：按消息类型递增对应计数器 */
static void stats_message(const char *kind) {
    if (!g_stats) {
        return;
    }
    pthread_mutex_lock(&g_stats->mutex);
    if (kind && strcmp(kind, "G") == 0) {
        g_stats->group_messages++;
    } else {
        g_stats->private_messages++;
    }
    pthread_mutex_unlock(&g_stats->mutex);
}

/* ======== 文件路径工具 ======== */

/* 拼接 data/ 目录下的文件路径 */
static void path_join(char *out, size_t cap, const char *name) {
    snprintf(out, cap, "%s/%s", DATA_DIR, name);
}

/*
 * valid_token — 验证标识符合法性
 *
 * 用户名、群名等标识符中不能包含 TAB、换行、回车和不可见控制字符。
 * 因为这些字符会破坏 TSV 文件格式和网络协议格式。
 */
static int valid_token(const char *s) {
    size_t n;
    if (!s || !*s) {
        return 0;
    }
    n = strlen(s);
    if (n >= QQ_MAX_NAME) {
        return 0;  /* 长度超出限制 */
    }
    for (size_t i = 0; i < n; i++) {
        unsigned char ch = (unsigned char)s[i];
        if (ch == '\t' || ch == '\r' || ch == '\n' || ch < 32) {
            return 0;  /* 包含非法控制字符 */
        }
    }
    return 1;
}

/* 带时间戳的日志输出（输出到 stderr） */
static void server_log(const char *fmt, ...) {
    va_list ap;
    time_t now = time(NULL);
    struct tm tmv;
    char ts[32];
    localtime_r(&now, &tmv);
    strftime(ts, sizeof(ts), "%H:%M:%S", &tmv);
    fprintf(stderr, "[%s] ", ts);
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fprintf(stderr, "\n");
}

/* ======== 任务队列操作（生产者-消费者模式） ======== */

/*
 * queue_push — accept 线程调用：将新连接放入任务队列
 *
 * 如果队列已满，生产者阻塞等待直到 worker 线程取出任务腾出空位。
 * 阻塞期间检查 g_running 标志，如果服务端正在关闭则直接丢弃连接。
 */
static void queue_push(int fd, const char *ip) {
    pthread_mutex_lock(&g_queue.mutex);
    /* 队列满时等待（has_job 条件变量同时用于"有任务"和"有空位"两种通知） */
    while (g_queue.count == MAX_QUEUE && g_running) {
        pthread_cond_wait(&g_queue.has_job, &g_queue.mutex);
    }
    if (!g_running) {
        /* 服务端正在关闭，放弃此连接 */
        pthread_mutex_unlock(&g_queue.mutex);
        close(fd);
        return;
    }
    /* 将新连接放入队尾 */
    g_queue.items[g_queue.tail].fd = fd;
    snprintf(g_queue.items[g_queue.tail].ip, sizeof(g_queue.items[g_queue.tail].ip), "%s", ip);
    g_queue.tail = (g_queue.tail + 1) % MAX_QUEUE;  /* 环形队列，取模回绕 */
    g_queue.count++;
    /* 广播通知所有等待的 worker 线程有任务到达 */
    pthread_cond_broadcast(&g_queue.has_job);
    pthread_mutex_unlock(&g_queue.mutex);
}

/*
 * queue_pop — worker 线程调用：从任务队列取出一个连接
 *
 * 如果队列为空，消费者阻塞等待直到 accept 线程放入新任务。
 * 返回 1 表示成功取出任务，返回 0 表示服务端正在关闭。
 */
static int queue_pop(Job *job) {
    pthread_mutex_lock(&g_queue.mutex);
    /* 队列空时等待 */
    while (g_queue.count == 0 && g_running) {
        pthread_cond_wait(&g_queue.has_job, &g_queue.mutex);
    }
    if (g_queue.count == 0) {
        /* 服务端正在关闭且队列已空 */
        pthread_mutex_unlock(&g_queue.mutex);
        return 0;
    }
    /* 从队首取出任务 */
    *job = g_queue.items[g_queue.head];
    g_queue.head = (g_queue.head + 1) % MAX_QUEUE;  /* 环形队列，取模回绕 */
    g_queue.count--;
    /* 广播通知可能正在等待空位的生产者（accept 线程） */
    pthread_cond_broadcast(&g_queue.has_job);
    pthread_mutex_unlock(&g_queue.mutex);
    return 1;
}

/* ======== 客户端通信基础函数 ======== */

/*
 * send_client — 线程安全地给客户端发送一行消息
 *
 * 使用每个客户端的 send_mutex 保护，防止多个线程同时给同一客户端
 * 发送消息时数据交错（例如："HELLO\nWORLD\n" 变成 "HELLOWORLD\n\n"）。
 */
static int send_client(Client *c, const char *line) {
    int rc;
    if (!c || c->fd < 0) {
        return -1;
    }
    pthread_mutex_lock(&c->send_mutex);   /* 获取该客户端的发送锁 */
    rc = qq_send_line(c->fd, line);
    pthread_mutex_unlock(&c->send_mutex); /* 释放锁 */
    return rc;
}

/* 格式化发送：类似 printf 风格的便捷封装 */
static int sendf_client(Client *c, const char *fmt, ...) {
    char line[QQ_MAX_LINE];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(line, sizeof(line), fmt, ap);
    va_end(ap);
    return send_client(c, line);
}

/* ======== 在线客户端管理 ======== */

/*
 * find_client_locked — 在在线客户端表中查找指定用户
 *
 * 调用前必须已持有 g_clients_mutex 锁。
 * 返回匹配的 Client 指针，未找到返回 NULL。
 */
static Client *find_client_locked(const char *user) {
    int i;
    for (i = 0; i < MAX_CLIENTS; i++) {
        if (g_clients[i].active && strcmp(g_clients[i].user, user) == 0) {
            return &g_clients[i];
        }
    }
    return NULL;
}

/* 统计当前在线客户端数量（调用前必须已持有 g_clients_mutex 锁） */
static int online_count_locked(void) {
    int i;
    int n = 0;
    for (i = 0; i < MAX_CLIENTS; i++) {
        if (g_clients[i].active) {
            n++;
        }
    }
    return n;
}

/* 判断指定用户是否在线（线程安全版本） */
static int is_online_user(const char *user) {
    int online;
    pthread_mutex_lock(&g_clients_mutex);
    online = find_client_locked(user) != NULL;
    pthread_mutex_unlock(&g_clients_mutex);
    return online;
}

/* 向所有在线客户端广播当前在线人数 */
static void broadcast_counts(void) {
    int i;
    int online;
    char line[128];
    pthread_mutex_lock(&g_clients_mutex);
    online = online_count_locked();
    snprintf(line, sizeof(line), "ONLINE\t%d", online);
    for (i = 0; i < MAX_CLIENTS; i++) {
        if (g_clients[i].active) {
            send_client(&g_clients[i], line);
        }
    }
    pthread_mutex_unlock(&g_clients_mutex);
}

/*
 * attach_client — 将新登录的客户端添加到在线表中
 *
 * 步骤：
 *   1. 检查该用户是否已在线（不允许重复登录）
 *   2. 在 g_clients[] 中找一个空闲槽位
 *   3. 初始化槽位并初始化 send_mutex
 *
 * 返回值：成功返回 0 并设置 *out，失败返回 -1
 */
static int attach_client(Client **out, int fd, const char *ip, const char *user,
                         const char *display, int peer_port) {
    int i;
    pthread_mutex_lock(&g_clients_mutex);

    /* 检查是否已在线 */
    if (find_client_locked(user)) {
        pthread_mutex_unlock(&g_clients_mutex);
        return -1;  /* 用户已在线，不允许重复登录 */
    }

    /* 查找空闲槽位 */
    for (i = 0; i < MAX_CLIENTS; i++) {
        if (!g_clients[i].active) {
            g_clients[i].fd = fd;
            g_clients[i].active = 1;  /* 标记槽位已占用 */
            g_clients[i].peer_port = peer_port;
            snprintf(g_clients[i].user, sizeof(g_clients[i].user), "%s", user);
            snprintf(g_clients[i].display, sizeof(g_clients[i].display), "%s", display);
            snprintf(g_clients[i].ip, sizeof(g_clients[i].ip), "%s", ip);
            pthread_mutex_init(&g_clients[i].send_mutex, NULL);  /* 初始化发送锁 */
            *out = &g_clients[i];
            pthread_mutex_unlock(&g_clients_mutex);
            return 0;
        }
    }

    /* 在线客户端已满 */
    pthread_mutex_unlock(&g_clients_mutex);
    return -1;
}

/*
 * detach_client — 将客户端从在线表中移除
 *
 * 客户端断开连接后调用，释放槽位供新连接使用。
 */
static void detach_client(Client *c) {
    if (!c) {
        return;
    }
    pthread_mutex_lock(&g_clients_mutex);
    c->active = 0;  /* 标记槽位空闲 */
    c->fd = -1;
    pthread_mutex_unlock(&g_clients_mutex);
    pthread_mutex_destroy(&c->send_mutex);  /* 销毁发送锁 */
}

/* ======== 用户数据持久化 ======== */

/*
 * user_exists_locked — 检查用户是否已注册
 * 调用前必须已持有 g_store_mutex 锁。
 */
static int user_exists_locked(const char *user) {
    char path[256];
    char line[512];
    FILE *fp;
    path_join(path, sizeof(path), "users.tsv");
    fp = fopen(path, "r");
    if (!fp) {
        return 0;  /* 文件不存在，说明还没有任何注册用户 */
    }
    while (fgets(line, sizeof(line), fp)) {
        char *name = strtok(line, "\t\r\n");  /* 取每行的第一个字段（用户名） */
        if (name && strcmp(name, user) == 0) {
            fclose(fp);
            return 1;  /* 找到匹配的用户 */
        }
    }
    fclose(fp);
    return 0;
}

/*
 * read_user_locked — 从 users.tsv 读取用户的显示名和密码
 *
 * TSV 格式：用户名\t转义后的显示名\t转义后的密码
 * 读取后自动调用 qq_unescape 解码。
 * 调用前必须已持有 g_store_mutex 锁。
 */
static int read_user_locked(const char *user, char *display, size_t display_cap,
                            char *password, size_t password_cap) {
    char path[256];
    char line[512];
    FILE *fp;
    path_join(path, sizeof(path), "users.tsv");
    fp = fopen(path, "r");
    if (!fp) {
        return 0;
    }
    while (fgets(line, sizeof(line), fp)) {
        char *name;
        char *display_enc;
        char *pass_enc;
        char *display_dec;
        char *pass_dec;
        qq_trim_newline(line);
        /* 解析 TSV 行：用户名 + 显示名 + 密码 */
        name = strtok(line, "\t");
        display_enc = strtok(NULL, "\t");
        pass_enc = strtok(NULL, "\t");
        if (!name || strcmp(name, user) != 0) {
            continue;  /* 不是目标用户，继续查找 */
        }
        /* 对转义后的字段进行解码 */
        display_dec = qq_unescape(display_enc ? display_enc : user);
        pass_dec = qq_unescape(pass_enc ? pass_enc : "");
        snprintf(display, display_cap, "%s", display_dec ? display_dec : user);
        snprintf(password, password_cap, "%s", pass_dec ? pass_dec : "");
        free(display_dec);
        free(pass_dec);
        fclose(fp);
        return 1;  /* 找到并成功读取 */
    }
    fclose(fp);
    return 0;  /* 未找到该用户 */
}

/*
 * create_user_locked — 注册新用户
 *
 * 将用户信息追加写入 users.tsv 文件。
 * 显示名和密码写入前进行百分号转义。
 * 调用前必须已持有 g_store_mutex 锁。
 */
static int create_user_locked(const char *user, const char *display, const char *password) {
    char path[256];
    FILE *fp;
    if (!user_exists_locked(user)) {
        path_join(path, sizeof(path), "users.tsv");
        fp = fopen(path, "a");  /* 追加模式写入 */
        if (fp) {
            char *ed = qq_escape(display);    /* 转义显示名 */
            char *ep = qq_escape(password);   /* 转义密码 */
            fprintf(fp, "%s\t%s\t%s\n", user, ed ? ed : display, ep ? ep : password);
            free(ed);
            free(ep);
            fclose(fp);
            return 1;
        }
    }
    return 0;  /* 用户已存在或文件写入失败 */
}

/* ======== 好友关系管理 ======== */

/*
 * are_friends_locked — 检查两个用户是否为好友关系
 *
 * friends.tsv 中双向存储：A→B 和 B→A 各一条记录。
 * 只需检查是否存在 A→B 的记录即可确认好友关系。
 * 调用前必须已持有 g_store_mutex 锁。
 */
static int are_friends_locked(const char *a, const char *b) {
    char path[256];
    char line[256];
    FILE *fp;
    path_join(path, sizeof(path), "friends.tsv");
    fp = fopen(path, "r");
    if (!fp) {
        return 0;
    }
    while (fgets(line, sizeof(line), fp)) {
        char *u = strtok(line, "\t\r\n");
        char *v = strtok(NULL, "\t\r\n");
        if (u && v && strcmp(u, a) == 0 && strcmp(v, b) == 0) {
            fclose(fp);
            return 1;  /* 找到 A→B 的记录 */
        }
    }
    fclose(fp);
    return 0;
}

/*
 * add_friend_pair — 添加双向好友关系
 *
 * 在 friends.tsv 中追加 A→B 和 B→A 两条记录。
 * 先检查关系是否已存在，避免重复写入。
 */
static void add_friend_pair(const char *a, const char *b) {
    char path[256];
    FILE *fp;
    if (!a || !b || !*a || !*b || strcmp(a, b) == 0) {
        return;  /* 不能添加自己为好友 */
    }
    pthread_mutex_lock(&g_store_mutex);
    path_join(path, sizeof(path), "friends.tsv");
    fp = fopen(path, "a");
    if (fp) {
        /* 双向写入（A→B 和 B→A），确保查询任意方向都能确认好友关系 */
        if (!are_friends_locked(a, b)) {
            fprintf(fp, "%s\t%s\n", a, b);
        }
        if (!are_friends_locked(b, a)) {
            fprintf(fp, "%s\t%s\n", b, a);
        }
        fclose(fp);
    }
    pthread_mutex_unlock(&g_store_mutex);
}

/*
 * remove_friend_pair — 删除双向好友关系
 *
 * 使用临时文件方案：读取原文件，跳过匹配的行，写入临时文件，最后 rename。
 * 这是标准的文件安全更新方式，避免写入中途崩溃导致数据损坏。
 */
static int remove_friend_pair(const char *a, const char *b) {
    char path[256];
    char tmp_path[512];
    char line[256];
    FILE *in;
    FILE *out;
    int removed = 0;

    pthread_mutex_lock(&g_store_mutex);
    path_join(path, sizeof(path), "friends.tsv");
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path);
    in = fopen(path, "r");
    out = fopen(tmp_path, "w");
    if (!in || !out) {
        if (in) fclose(in);
        if (out) fclose(out);
        remove(tmp_path);
        pthread_mutex_unlock(&g_store_mutex);
        return 0;
    }
    /* 逐行读取，跳过要删除的关系 */
    while (fgets(line, sizeof(line), in)) {
        char original[256];
        char *u;
        char *v;
        snprintf(original, sizeof(original), "%s", line);  /* 保留原始行 */
        u = strtok(line, "\t\r\n");
        v = strtok(NULL, "\t\r\n");
        if (u && v &&
            ((strcmp(u, a) == 0 && strcmp(v, b) == 0) ||
             (strcmp(u, b) == 0 && strcmp(v, a) == 0))) {
            removed = 1;  /* 匹配到了，跳过不写入（删除双向关系） */
        } else {
            fputs(original, out);  /* 不匹配，保留 */
        }
    }
    fclose(in);
    fclose(out);
    /* 原子替换原文件 */
    rename(tmp_path, path);
    pthread_mutex_unlock(&g_store_mutex);
    return removed;
}

/* ======== 好友申请管理 ======== */

/*
 * friend_request_exists_locked — 检查好友申请是否已存在
 * 调用前必须已持有 g_store_mutex 锁。
 */
static int friend_request_exists_locked(const char *from, const char *to) {
    char path[256];
    char line[256];
    FILE *fp;
    path_join(path, sizeof(path), "friend_requests.tsv");
    fp = fopen(path, "r");
    if (!fp) {
        return 0;
    }
    while (fgets(line, sizeof(line), fp)) {
        char *f = strtok(line, "\t\r\n");
        char *t = strtok(NULL, "\t\r\n");
        if (f && t && strcmp(f, from) == 0 && strcmp(t, to) == 0) {
            fclose(fp);
            return 1;  /* 已存在相同的申请 */
        }
    }
    fclose(fp);
    return 0;
}

/* 添加一条好友申请记录 */
static void add_friend_request_locked(const char *from, const char *to) {
    char path[256];
    FILE *fp;
    if (friend_request_exists_locked(from, to)) {
        return;  /* 防止重复申请 */
    }
    path_join(path, sizeof(path), "friend_requests.tsv");
    fp = fopen(path, "a");
    if (fp) {
        fprintf(fp, "%s\t%s\n", from, to);
        fclose(fp);
    }
}

/* 删除一条好友申请记录（使用临时文件安全更新） */
static int remove_friend_request(const char *from, const char *to) {
    char path[256];
    char tmp_path[512];
    char line[256];
    FILE *in;
    FILE *out;
    int removed = 0;

    pthread_mutex_lock(&g_store_mutex);
    path_join(path, sizeof(path), "friend_requests.tsv");
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path);
    in = fopen(path, "r");
    out = fopen(tmp_path, "w");
    if (!in || !out) {
        if (in) fclose(in);
        if (out) fclose(out);
        pthread_mutex_unlock(&g_store_mutex);
        return 0;
    }
    while (fgets(line, sizeof(line), in)) {
        char original[256];
        char *f;
        char *t;
        snprintf(original, sizeof(original), "%s", line);
        f = strtok(line, "\t\r\n");
        t = strtok(NULL, "\t\r\n");
        if (f && t && strcmp(f, from) == 0 && strcmp(t, to) == 0) {
            removed = 1;  /* 匹配到目标申请，跳过（删除） */
        } else {
            fputs(original, out);  /* 保留其他申请 */
        }
    }
    fclose(in);
    fclose(out);
    rename(tmp_path, path);
    pthread_mutex_unlock(&g_store_mutex);
    return removed;
}

/* ======== 消息持久化 ======== */

/*
 * store_message — 将消息保存到 messages.tsv 历史记录
 *
 * 所有聊天消息（私聊和群聊）都追加写入 messages.tsv。
 * 这是历史消息查询和关键字检索的数据来源。
 *
 * TSV 格式：时间戳\t消息类型\t发送者\t接收者(群名)\t转义后的正文
 */
static void store_message(const char *kind, const char *from, const char *to,
                          const char *text) {
    char path[256];
    FILE *fp;
    char *et = qq_escape(text);  /* 转义消息正文 */
    pthread_mutex_lock(&g_store_mutex);
    path_join(path, sizeof(path), "messages.tsv");
    fp = fopen(path, "a");  /* 追加模式 */
    if (fp) {
        fprintf(fp, "%ld\t%s\t%s\t%s\t%s\n", (long)time(NULL), kind, from, to, et ? et : text);
        fclose(fp);
    }
    pthread_mutex_unlock(&g_store_mutex);
    free(et);
}

/*
 * store_offline — 将离线消息保存到 offline.tsv
 *
 * 当目标用户不在线时，私聊和群聊消息会存入离线文件。
 * 用户下次登录时由 deliver_offline() 投递并删除。
 *
 * TSV 格式：目标用户\t消息类型\t发送者\t作用域\t时间戳\t转义后的正文
 */
static void store_offline(const char *to_user, const char *kind, const char *from,
                          const char *scope, const char *text) {
    char path[256];
    FILE *fp;
    char *et = qq_escape(text);
    pthread_mutex_lock(&g_store_mutex);
    path_join(path, sizeof(path), "offline.tsv");
    fp = fopen(path, "a");
    if (fp) {
        fprintf(fp, "%s\t%s\t%s\t%s\t%ld\t%s\n",
                to_user, kind, from, scope, (long)time(NULL), et ? et : text);
        fclose(fp);
    }
    pthread_mutex_unlock(&g_store_mutex);
    free(et);
}

/*
 * deliver_offline — 用户登录后投递离线消息
 *
 * 读取 offline.tsv，将属于当前用户的离线消息逐条发送，
 * 发送后从文件中删除（使用临时文件方案）。
 *
 * 这样即使客户端不在线，消息也不会丢失。
 */
static void deliver_offline(Client *c) {
    char path[256];
    char tmp_path[512];
    char line[QQ_MAX_LINE];
    FILE *in;
    FILE *out;

    pthread_mutex_lock(&g_store_mutex);
    path_join(path, sizeof(path), "offline.tsv");
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path);
    in = fopen(path, "r");
    out = fopen(tmp_path, "w");
    if (!in || !out) {
        if (in) fclose(in);
        if (out) fclose(out);
        pthread_mutex_unlock(&g_store_mutex);
        return;
    }
    while (fgets(line, sizeof(line), in)) {
        char original[QQ_MAX_LINE];
        char *to;
        char *kind;
        char *from;
        char *scope;
        char *ts;
        char *text;
        snprintf(original, sizeof(original), "%s", line);
        qq_trim_newline(line);
        to = strtok(line, "\t");
        kind = strtok(NULL, "\t");
        from = strtok(NULL, "\t");
        scope = strtok(NULL, "\t");
        ts = strtok(NULL, "\t");
        text = strtok(NULL, "\t\r\n");
        /* 检查是否是给当前用户的离线消息 */
        if (to && kind && from && scope && ts && text && strcmp(to, c->user) == 0) {
            /* 投递离线消息给客户端 */
            char *dt = qq_unescape(text);
            char *et = qq_escape(dt ? dt : text);
            sendf_client(c, "OFFLINE\t%s\t%s\t%s\t%s\t%s",
                         kind, from, scope, ts, et ? et : text);
            free(dt);
            free(et);
            /* 不写入临时文件 = 删除此条离线消息 */
        } else {
            fputs(original, out);  /* 保留不属于当前用户的离线消息 */
        }
    }
    fclose(in);
    fclose(out);
    rename(tmp_path, path);  /* 原子替换 */
    pthread_mutex_unlock(&g_store_mutex);
}

/* ======== 好友/群统计与列表 ======== */

/* 获取用户的好友总数 */
static int get_friend_count(const char *user) {
    char path[256];
    char line[256];
    FILE *fp;
    int n = 0;
    pthread_mutex_lock(&g_store_mutex);
    path_join(path, sizeof(path), "friends.tsv");
    fp = fopen(path, "r");
    if (fp) {
        while (fgets(line, sizeof(line), fp)) {
            char *u = strtok(line, "\t\r\n");
            char *v = strtok(NULL, "\t\r\n");
            if (u && v && strcmp(u, user) == 0) {
                n++;  /* 统计该用户添加的好友数 */
            }
        }
        fclose(fp);
    }
    pthread_mutex_unlock(&g_store_mutex);
    return n;
}

/* 获取用户当前在线好友的数量（在线状态查询需要持有 g_clients_mutex） */
static int get_online_friend_count(const char *user) {
    char path[256];
    char line[256];
    FILE *fp;
    int n = 0;
    pthread_mutex_lock(&g_store_mutex);
    path_join(path, sizeof(path), "friends.tsv");
    fp = fopen(path, "r");
    if (fp) {
        while (fgets(line, sizeof(line), fp)) {
            char *u = strtok(line, "\t\r\n");
            char *v = strtok(NULL, "\t\r\n");
            if (u && v && strcmp(u, user) == 0 && is_online_user(v)) {
                n++;  /* 好友在线则计数 */
            }
        }
        fclose(fp);
    }
    pthread_mutex_unlock(&g_store_mutex);
    return n;
}

/*
 * send_friend_list — 向客户端发送完整好友列表
 *
 * 协议格式：
 *   FRIENDS_BEGIN
 *   FRIEND\t好友名\t是否在线
 *   FRIEND\t好友名2\t是否在线
 *   ...
 *   FRIENDS_END\t总数
 */
static void send_friend_list(Client *c) {
    char path[256];
    char line[256];
    FILE *fp;
    int friends = 0;

    send_client(c, "FRIENDS_BEGIN");
    pthread_mutex_lock(&g_store_mutex);
    path_join(path, sizeof(path), "friends.tsv");
    fp = fopen(path, "r");
    if (fp) {
        while (fgets(line, sizeof(line), fp)) {
            char *u = strtok(line, "\t\r\n");
            char *v = strtok(NULL, "\t\r\n");
            if (u && v && strcmp(u, c->user) == 0) {
                int online = is_online_user(v);  /* 查询好友是否在线 */
                sendf_client(c, "FRIEND\t%s\t%d", v, online);
                friends++;
            }
        }
        fclose(fp);
    }
    pthread_mutex_unlock(&g_store_mutex);
    sendf_client(c, "FRIENDS_END\t%d", friends);
}

/* 向客户端发送好友统计信息（好友总数 + 在线好友数） */
static void send_counts(Client *c) {
    int friends = get_friend_count(c->user);
    int online_friends = get_online_friend_count(c->user);
    sendf_client(c, "COUNTS\t%d\t%d", friends, online_friends);
}

static void remember_refresh_user(char users[][QQ_MAX_NAME], int *count, const char *user) {
    int i;
    if (!user || !*user || *count >= MAX_CLIENTS) {
        return;
    }
    for (i = 0; i < *count; i++) {
        if (strcmp(users[i], user) == 0) {
            return;
        }
    }
    snprintf(users[*count], QQ_MAX_NAME, "%s", user);
    (*count)++;
}

/* ======== 群管理 ======== */

/*
 * join_group — 将用户加入群
 *
 * 先在 groups.tsv 中检查是否已经是群成员，避免重复加入。
 * 如果不是成员则追加新记录。
 * "lobby" 是已废弃的公共群，不允许加入。
 */
static void join_group(const char *group, const char *user) {
    char path[256];
    FILE *fp;
    if (!group || !*group || !user || !*user) {
        return;
    }
    pthread_mutex_lock(&g_store_mutex);
    path_join(path, sizeof(path), "groups.tsv");
    /* 先检查是否已是成员 */
    fp = fopen(path, "r");
    if (fp) {
        char line[256];
        while (fgets(line, sizeof(line), fp)) {
            char *g = strtok(line, "\t\r\n");
            char *u = strtok(NULL, "\t\r\n");
            if (g && u && strcmp(g, group) == 0 && strcmp(u, user) == 0) {
                fclose(fp);
                pthread_mutex_unlock(&g_store_mutex);
                return;  /* 已经是成员，无需操作 */
            }
        }
        fclose(fp);
    }
    /* 追加新成员记录 */
    fp = fopen(path, "a");
    if (fp) {
        fprintf(fp, "%s\t%s\n", group, user);
        fclose(fp);
    }
    pthread_mutex_unlock(&g_store_mutex);
}

/* 从群中移除一个成员（使用临时文件安全更新） */
static int remove_group_member(const char *group, const char *user) {
    char path[256];
    char tmp_path[512];
    char line[256];
    FILE *in;
    FILE *out;
    int removed = 0;

    pthread_mutex_lock(&g_store_mutex);
    path_join(path, sizeof(path), "groups.tsv");
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path);
    in = fopen(path, "r");
    out = fopen(tmp_path, "w");
    if (!in || !out) {
        if (in) fclose(in);
        if (out) fclose(out);
        remove(tmp_path);
        pthread_mutex_unlock(&g_store_mutex);
        return 0;
    }
    while (fgets(line, sizeof(line), in)) {
        char original[256];
        char *g;
        char *u;
        snprintf(original, sizeof(original), "%s", line);
        g = strtok(line, "\t\r\n");
        u = strtok(NULL, "\t\r\n");
        if (g && u && strcmp(g, group) == 0 && strcmp(u, user) == 0) {
            removed = 1;  /* 匹配的成员记录，跳过（删除） */
        } else {
            fputs(original, out);
        }
    }
    fclose(in);
    fclose(out);
    rename(tmp_path, path);
    pthread_mutex_unlock(&g_store_mutex);
    return removed;
}

/* 检查用户是否是指定群的成员 */
static int is_group_member_locked(const char *group, const char *user) {
    char path[256];
    char line[256];
    FILE *fp;
    int found = 0;
    if (strcmp(group, "lobby") == 0) {
        return 0;  /* lobby 群已废弃 */
    }
    path_join(path, sizeof(path), "groups.tsv");
    fp = fopen(path, "r");
    if (!fp) {
        return 0;
    }
    while (fgets(line, sizeof(line), fp)) {
        char *g = strtok(line, "\t\r\n");
        char *u = strtok(NULL, "\t\r\n");
        if (g && u && strcmp(g, group) == 0 && strcmp(u, user) == 0) {
            found = 1;
            break;
        }
    }
    fclose(fp);
    return found;
}

/* 检查群是否已存在（至少有一个成员） */
static int group_exists_locked(const char *group) {
    char path[256];
    char line[256];
    FILE *fp;
    if (strcmp(group, "lobby") == 0) {
        return 0;  /* lobby 群已废弃 */
    }
    path_join(path, sizeof(path), "groups.tsv");
    fp = fopen(path, "r");
    if (!fp) {
        return 0;
    }
    while (fgets(line, sizeof(line), fp)) {
        char *g = strtok(line, "\t\r\n");
        if (g && strcmp(g, group) == 0) {
            fclose(fp);
            return 1;  /* 找到至少一个群成员，说明群存在 */
        }
    }
    fclose(fp);
    return 0;
}

/* ======== 群邀请管理 ======== */

static int group_invite_exists_locked(const char *group, const char *from, const char *to) {
    char path[256];
    char line[256];
    FILE *fp;
    path_join(path, sizeof(path), "group_invites.tsv");
    fp = fopen(path, "r");
    if (!fp) {
        return 0;
    }
    while (fgets(line, sizeof(line), fp)) {
        char *g = strtok(line, "\t\r\n");
        char *f = strtok(NULL, "\t\r\n");
        char *t = strtok(NULL, "\t\r\n");
        if (g && f && t && strcmp(g, group) == 0 && strcmp(f, from) == 0 && strcmp(t, to) == 0) {
            fclose(fp);
            return 1;
        }
    }
    fclose(fp);
    return 0;
}

static void add_group_invite_locked(const char *group, const char *from, const char *to) {
    char path[256];
    FILE *fp;
    if (group_invite_exists_locked(group, from, to)) {
        return;  /* 已存在相同的邀请 */
    }
    path_join(path, sizeof(path), "group_invites.tsv");
    fp = fopen(path, "a");
    if (fp) {
        fprintf(fp, "%s\t%s\t%s\n", group, from, to);
        fclose(fp);
    }
}

/* 删除一条群邀请记录 */
static int remove_group_invite(const char *group, const char *from, const char *to) {
    char path[256];
    char tmp_path[512];
    char line[256];
    FILE *in;
    FILE *out;
    int removed = 0;

    pthread_mutex_lock(&g_store_mutex);
    path_join(path, sizeof(path), "group_invites.tsv");
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path);
    in = fopen(path, "r");
    out = fopen(tmp_path, "w");
    if (!in || !out) {
        if (in) fclose(in);
        if (out) fclose(out);
        pthread_mutex_unlock(&g_store_mutex);
        return 0;
    }
    while (fgets(line, sizeof(line), in)) {
        char original[256];
        char *g;
        char *f;
        char *t;
        snprintf(original, sizeof(original), "%s", line);
        g = strtok(line, "\t\r\n");
        f = strtok(NULL, "\t\r\n");
        t = strtok(NULL, "\t\r\n");
        if (g && f && t && strcmp(g, group) == 0 && strcmp(f, from) == 0 && strcmp(t, to) == 0) {
            removed = 1;  /* 匹配的邀请记录，跳过（删除） */
        } else {
            fputs(original, out);
        }
    }
    fclose(in);
    fclose(out);
    rename(tmp_path, path);
    pthread_mutex_unlock(&g_store_mutex);
    return removed;
}

/* 向客户端发送用户的群列表 */
static void send_group_list(Client *c) {
    char path[256];
    char line[256];
    char groups[256][QQ_MAX_NAME];  /* 去重用的临时数组 */
    FILE *fp;
    int count = 0;

    pthread_mutex_lock(&g_store_mutex);
    path_join(path, sizeof(path), "groups.tsv");
    fp = fopen(path, "r");
    if (fp) {
        while (count < 256 && fgets(line, sizeof(line), fp)) {
            char *g = strtok(line, "\t\r\n");
            char *u = strtok(NULL, "\t\r\n");
            int exists = 0;
            /* 只列出当前用户所在的群，排除废弃的 lobby */
            if (!g || !u || strcmp(u, c->user) != 0 || strcmp(g, "lobby") == 0) {
                continue;
            }
            /* 去重：同一个群可能有多条成员记录 */
            for (int i = 0; i < count; i++) {
                if (strcmp(groups[i], g) == 0) {
                    exists = 1;
                    break;
                }
            }
            if (!exists) {
                snprintf(groups[count++], QQ_MAX_NAME, "%s", g);
            }
        }
        fclose(fp);
    }
    pthread_mutex_unlock(&g_store_mutex);

    /* 发送群列表 */
    send_client(c, "GROUPS_BEGIN");
    for (int i = 0; i < count; i++) {
        sendf_client(c, "GROUP\t%s", groups[i]);
    }
    sendf_client(c, "GROUPS_END\t%d", count);
}

/* ======== 消息路由（核心逻辑） ======== */

/*
 * route_private — 私聊消息路由
 *
 * 处理流程：
 *   1. 将消息持久化到 messages.tsv
 *   2. 在在线表中查找目标用户
 *   3. 如果目标在线 → 直接转发
 *   4. 如果目标离线 → 存入 offline.tsv 等下次登录投递
 *   5. 将消息回显给发送者（确认发送成功）
 *
 * 这是私聊的核心：不是广播，只发给指定的一个目标用户。
 */
static void route_private(Client *sender, const char *to, const char *text) {
    char *et = qq_escape(text);
    Client *target = NULL;

    /* 持久化存储 */
    store_message("P", sender->user, to, text);

    /* 查找目标是否在线 */
    pthread_mutex_lock(&g_clients_mutex);
    target = find_client_locked(to);
    if (target) {
        /* 目标在线：直接转发 */
        sendf_client(target, "MSG\tP\t%s\t%s\t%ld\t%s",
                     sender->user, to, (long)time(NULL), et ? et : text);
    }
    pthread_mutex_unlock(&g_clients_mutex);

    /* 目标离线：写入离线消息文件 */
    if (!target) {
        store_offline(to, "P", sender->user, to, text);
    }

    /* 回显给发送者 */
    sendf_client(sender, "MSG\tP\t%s\t%s\t%ld\t%s",
                 sender->user, to, (long)time(NULL), et ? et : text);
    free(et);
}

/*
 * route_group — 群聊消息路由
 *
 * 处理流程：
 *   1. 持久化到 messages.tsv
 *   2. 从 groups.tsv 读取群成员列表
 *   3. 对每个群成员：在线则直接转发，离线则写入离线消息
 *
 * 关键设计：
 *   - 群聊不是 UDP 广播！是从 groups.tsv 查出群成员后定向转发
 *   - 只有群成员能收到群消息，体现"群是部分人之间的聊天"
 *   - 发送者自己不存离线消息（因为已经回显了）
 */
static void route_group(Client *sender, const char *group, const char *text) {
    char path[256];
    char line[256];
    FILE *fp;
    char members[256][QQ_MAX_NAME];  /* 群成员列表（去重后） */
    int count = 0;
    int i;
    char *et = qq_escape(text);

    store_message("G", sender->user, group, text);

    /* 从 groups.tsv 读取群成员 */
    pthread_mutex_lock(&g_store_mutex);
    path_join(path, sizeof(path), "groups.tsv");
    fp = fopen(path, "r");
    if (fp) {
        while (count < 256 && fgets(line, sizeof(line), fp)) {
            char *g = strtok(line, "\t\r\n");
            char *u = strtok(NULL, "\t\r\n");
            if (g && u && strcmp(g, group) == 0) {
                int exists = 0;
                int j;
                /* 去重 */
                for (j = 0; j < count; j++) {
                    if (strcmp(members[j], u) == 0) {
                        exists = 1;
                        break;
                    }
                }
                if (!exists) {
                    snprintf(members[count++], QQ_MAX_NAME, "%s", u);
                }
            }
        }
        fclose(fp);
    }
    pthread_mutex_unlock(&g_store_mutex);

    /* 逐成员转发 */
    for (i = 0; i < count; i++) {
        Client *target = NULL;
        pthread_mutex_lock(&g_clients_mutex);
        target = find_client_locked(members[i]);
        if (target) {
            /* 成员在线：直接转发 */
            sendf_client(target, "MSG\tG\t%s\t%s\t%ld\t%s",
                         sender->user, group, (long)time(NULL), et ? et : text);
        }
        pthread_mutex_unlock(&g_clients_mutex);
        /* 成员离线且不是发送者自己：写入离线消息 */
        if (!target && strcmp(members[i], sender->user) != 0) {
            store_offline(members[i], "G", sender->user, group, text);
        }
    }
    free(et);
}

/* ======== 历史消息查询 ======== */

/*
 * 兼容旧消息格式的类型判断。
 * messages.tsv 中可能存有旧版本软件写入的不同格式标识，
 * 这里统一兼容处理。
 */
static int message_kind_is_private(const char *kind) {
    return kind &&
           (strcmp(kind, "P") == 0 ||
            strcmp(kind, "MSGP") == 0 ||
            strcmp(kind, "PRIVATE") == 0 ||
            strcmp(kind, "private") == 0);
}

static int message_kind_is_group(const char *kind) {
    return kind &&
           (strcmp(kind, "G") == 0 ||
            strcmp(kind, "MSGG") == 0 ||
            strcmp(kind, "GROUP") == 0 ||
            strcmp(kind, "group") == 0);
}

/*
 * history_line_matches — 判断一条历史消息对当前用户是否可见
 *
 * 权限判断规则：
 *   - ALL 模式：返回用户参与的所有消息（私聊双方之一 + 所在群的消息）
 *   - P 模式：只返回指定会话的私聊消息（用户必须是收发双方之一）
 *   - G 模式：只返回指定群的消息（用户必须是该群成员）
 *
 * 这是"消息权限"的核心实现：不能返回全站消息，只能返回用户有权查看的。
 */
static int history_line_matches(Client *c, const char *want_kind, const char *scope,
                                const char *kind, const char *from, const char *to) {
    int private_like = message_kind_is_private(kind);
    int group_like = message_kind_is_group(kind);

    /* 兼容旧格式：无法从 kind 识别时，根据 from/to 推断 */
    if (!private_like && !group_like &&
        (strcmp(from, c->user) == 0 || strcmp(to, c->user) == 0)) {
        private_like = 1;
    }

    /* ALL 模式：返回所有关联消息 */
    if (strcmp(want_kind, "ALL") == 0) {
        if (private_like) {
            return strcmp(from, c->user) == 0 || strcmp(to, c->user) == 0;
        }
        if (group_like) {
            return is_group_member_locked(to, c->user);  /* to 字段即群名 */
        }
        return 0;
    }

    /* 私聊历史：用户必须是收发双方之一 */
    if (strcmp(want_kind, "P") == 0 && private_like) {
        if (strcmp(scope, "*") == 0) {
            return strcmp(from, c->user) == 0 || strcmp(to, c->user) == 0;
        }
        return ((strcmp(from, c->user) == 0 && strcmp(to, scope) == 0) ||
                (strcmp(from, scope) == 0 && strcmp(to, c->user) == 0));
    }

    /* 群聊历史：用户必须是该群成员 */
    if (strcmp(want_kind, "G") == 0 && group_like) {
        if (strcmp(scope, "*") == 0) {
            return is_group_member_locked(to, c->user);
        }
        return strcmp(to, scope) == 0;
    }

    return 0;
}

/*
 * send_history — 查询并发送历史消息
 *
 * 使用环形缓冲区（ring buffer）存储最后 limit 条匹配记录。
 * 好处：
 *   1. 不需要将 messages.tsv 全部读入内存
 *   2. 内存占用固定（limit * QQ_MAX_LINE）
 *   3. 自动保留最新的 limit 条记录
 *
 * 参数：
 *   kind:  "P" 私聊 / "G" 群聊 / "ALL" 全部
 *   scope: 具体的好友名/群名 / "*" 全部 / "*" 所有会话
 *   limit: 最多返回多少条
 */
static void send_history(Client *c, const char *kind, const char *scope, int limit) {
    char path[256];
    char line[QQ_MAX_LINE];
    char (*ring)[QQ_MAX_LINE];  /* 环形缓冲区 */
    FILE *fp;
    int n = 0;
    int i;
    int start;
    int sent;

    if (limit <= 0) {
        limit = 100;  /* 默认返回 100 条 */
    }
    if (limit > 1000) {
        limit = 1000; /* 上限 1000 条，防止内存占用过大 */
    }

    /* 分配环形缓冲区 */
    ring = calloc((size_t)limit, sizeof(*ring));
    if (!ring) {
        send_client(c, "HISTORY_BEGIN");
        send_client(c, "HISTORY_END\t0");
        return;
    }

    /* 逐行读取 messages.tsv，过滤匹配的记录 */
    pthread_mutex_lock(&g_store_mutex);
    path_join(path, sizeof(path), "messages.tsv");
    fp = fopen(path, "r");
    if (fp) {
        while (fgets(line, sizeof(line), fp)) {
            char copy[QQ_MAX_LINE];
            char *ts;
            char *k;
            char *from;
            char *to;
            char *text;
            snprintf(copy, sizeof(copy), "%s", line);
            qq_trim_newline(copy);
            ts = strtok(copy, "\t");
            k = strtok(NULL, "\t");
            from = strtok(NULL, "\t");
            to = strtok(NULL, "\t");
            text = strtok(NULL, "\t\r\n");
            /* 权限检查：当前用户是否有权查看此条消息 */
            if (ts && k && from && to && text &&
                history_line_matches(c, kind, scope, k, from, to)) {
                /* 存入环形缓冲区（n % limit 实现循环覆盖） */
                snprintf(ring[n % limit], sizeof(ring[0]), "%s", line);
                n++;
            }
        }
        fclose(fp);
    }
    pthread_mutex_unlock(&g_store_mutex);

    /* 发送历史消息 */
    send_client(c, "HISTORY_BEGIN");
    start = n > limit ? n - limit : 0;  /* 只发送最新的 limit 条 */
    for (i = start; i < n; i++) {
        char copy[QQ_MAX_LINE];
        char *ts;
        char *k;
        char *from;
        char *to;
        char *text;
        snprintf(copy, sizeof(copy), "%s", ring[i % limit]);
        qq_trim_newline(copy);
        ts = strtok(copy, "\t");
        k = strtok(NULL, "\t");
        from = strtok(NULL, "\t");
        to = strtok(NULL, "\t");
        text = strtok(NULL, "\t\r\n");
        if (ts && k && from && to && text) {
            sendf_client(c, "HISTORY\t%s\t%s\t%s\t%s\t%s", k, from, to, ts, text);
        }
    }
    sent = n > limit ? limit : n;
    sendf_client(c, "HISTORY_END\t%d", sent);
    free(ring);
}

/*
 * send_search — 关键字检索历史消息
 *
 * 与 send_history 共用同一套权限判断逻辑（history_line_matches），
 * 在此基础上对消息正文做子串匹配。
 * 最多返回 80 条匹配结果。
 */
static void send_search(Client *c, const char *keyword) {
    char path[256];
    char line[QQ_MAX_LINE];
    FILE *fp;
    int sent = 0;
    char *ekey = qq_escape(keyword);

    (void)ekey;
    send_client(c, "SEARCH_BEGIN");
    pthread_mutex_lock(&g_store_mutex);
    path_join(path, sizeof(path), "messages.tsv");
    fp = fopen(path, "r");
    if (fp) {
        while (sent < 80 && fgets(line, sizeof(line), fp)) {
            char copy[QQ_MAX_LINE];
            char *ts;
            char *kind;
            char *from;
            char *to;
            char *text;
            char *dt;
            snprintf(copy, sizeof(copy), "%s", line);
            qq_trim_newline(copy);
            ts = strtok(copy, "\t");
            kind = strtok(NULL, "\t");
            from = strtok(NULL, "\t");
            to = strtok(NULL, "\t");
            text = strtok(NULL, "\t\r\n");
            if (!ts || !kind || !from || !to || !text) {
                continue;  /* 格式不完整，跳过 */
            }
            /* 权限检查：只能检索自己有权查看的消息 */
            if (!history_line_matches(c, "ALL", "*", kind, from, to)) {
                continue;
            }
            /* 解码后做子串匹配 */
            dt = qq_unescape(text);
            if (dt && strstr(dt, keyword)) {
                sendf_client(c, "SEARCH\t%s\t%s\t%s\t%s\t%s", kind, from, to, ts, text);
                sent++;
            }
            free(dt);
        }
        fclose(fp);
    }
    pthread_mutex_unlock(&g_store_mutex);
    sendf_client(c, "SEARCH_END\t%d", sent);
    free(ekey);
}

/* ======== P2P 文件传输支持 ======== */

/*
 * send_peer_info — 查询目标用户的 P2P 文件传输地址
 *
 * 服务端只负责返回目标用户的 IP 地址和文件监听端口。
 * 实际的文件数据传输由两个客户端直接建立 TCP 连接完成，
 * 服务端不参与文件数据中转。
 */
static void send_peer_info(Client *c, const char *who) {
    pthread_mutex_lock(&g_clients_mutex);
    Client *target = find_client_locked(who);
    if (target) {
        /* 目标在线：返回其 IP 和 P2P 端口 */
        sendf_client(c, "PEER\t%s\t%s\t%d\t1", target->user, target->ip, target->peer_port);
    } else {
        /* 目标离线：返回无效地址 */
        sendf_client(c, "PEER\t%s\t0.0.0.0\t0\t0", who);
    }
    pthread_mutex_unlock(&g_clients_mutex);
}

/* 文件传输通知（通过私聊消息告知对方有文件要传输） */
static void route_file_notice(Client *sender, const char *to, const char *filename, const char *size) {
    char text[512];
    snprintf(text, sizeof(text), "[file] %s sent %s bytes", filename, size);
    route_private(sender, to, text);
}

/* ======== 通知与工具函数 ======== */

/* 发送系统通知给指定客户端 */
static void send_notice(Client *c, const char *fmt, ...) {
    char text[512];
    char *et;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(text, sizeof(text), fmt, ap);
    va_end(ap);
    et = qq_escape(text);
    sendf_client(c, "NOTICE\t%ld\t%s", (long)time(NULL), et ? et : text);
    free(et);
}

/* 向指定在线用户发送一条原始协议行（不转义） */
static int notify_online_line(const char *user, const char *line) {
    Client *target = NULL;
    pthread_mutex_lock(&g_clients_mutex);
    target = find_client_locked(user);
    if (target) {
        send_client(target, line);
    }
    pthread_mutex_unlock(&g_clients_mutex);
    return target != NULL;
}

/* 向指定在线用户发送一条格式化通知 */
static void notify_online_notice(const char *user, const char *fmt, ...) {
    char text[512];
    char line[QQ_MAX_LINE];
    char *et;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(text, sizeof(text), fmt, ap);
    va_end(ap);
    et = qq_escape(text);
    snprintf(line, sizeof(line), "NOTICE\t%ld\t%s", (long)time(NULL), et ? et : text);
    notify_online_line(user, line);
    free(et);
}

/*
 * refresh_user_state — 刷新用户状态
 *
 * 当好友关系或群成员关系发生变化时，重新向用户发送
 * 最新的好友列表、统计数据和群列表。
 */
static void refresh_user_state(const char *user) {
    Client *target = NULL;
    pthread_mutex_lock(&g_clients_mutex);
    target = find_client_locked(user);
    pthread_mutex_unlock(&g_clients_mutex);
    if (target && target->active) {
        send_friend_list(target);
        send_counts(target);
        send_group_list(target);
    }
}

static void refresh_friends_of_user(const char *user) {
    char path[256];
    char line[256];
    char users[MAX_CLIENTS][QQ_MAX_NAME];
    FILE *fp;
    int count = 0;
    int i;

    if (!user || !*user) {
        return;
    }

    pthread_mutex_lock(&g_store_mutex);
    path_join(path, sizeof(path), "friends.tsv");
    fp = fopen(path, "r");
    if (fp) {
        while (fgets(line, sizeof(line), fp)) {
            char *u = strtok(line, "\t\r\n");
            char *v = strtok(NULL, "\t\r\n");
            if (u && v && strcmp(v, user) == 0) {
                remember_refresh_user(users, &count, u);
            }
        }
        fclose(fp);
    }
    pthread_mutex_unlock(&g_store_mutex);

    for (i = 0; i < count; i++) {
        refresh_user_state(users[i]);
    }
}

/*
 * deliver_pending_requests — 登录时投递待处理的好友申请和群邀请
 *
 * 用户登录后，检查是否有未处理的好友申请和群邀请，
 * 逐条发送给用户，让用户决定同意还是拒绝。
 */
static void deliver_pending_requests(Client *c) {
    char path[256];
    char line[256];
    FILE *fp;

    /* 投递待处理的好友申请 */
    pthread_mutex_lock(&g_store_mutex);
    path_join(path, sizeof(path), "friend_requests.tsv");
    fp = fopen(path, "r");
    if (fp) {
        while (fgets(line, sizeof(line), fp)) {
            char *from = strtok(line, "\t\r\n");
            char *to = strtok(NULL, "\t\r\n");
            if (from && to && strcmp(to, c->user) == 0) {
                sendf_client(c, "FRIEND_REQUEST\t%s", from);
            }
        }
        fclose(fp);
    }

    /* 投递待处理的群邀请 */
    path_join(path, sizeof(path), "group_invites.tsv");
    fp = fopen(path, "r");
    if (fp) {
        while (fgets(line, sizeof(line), fp)) {
            char *group = strtok(line, "\t\r\n");
            char *from = strtok(NULL, "\t\r\n");
            char *to = strtok(NULL, "\t\r\n");
            if (group && from && to && strcmp(group, "lobby") != 0 && strcmp(to, c->user) == 0) {
                sendf_client(c, "GROUP_INVITE\t%s\t%s", group, from);
            }
        }
        fclose(fp);
    }
    pthread_mutex_unlock(&g_store_mutex);
}

/* ======== 业务命令处理函数 ======== */

/*
 * request_friend — 处理好友申请
 *
 * 校验：
 *   1. 目标账号格式合法
 *   2. 不能添加自己
 *   3. 目标账号必须存在
 *   4. 不能是已有好友
 *   5. 不能重复申请
 *
 * 满足条件后写入 friend_requests.tsv，如果目标在线则实时通知。
 */
static void request_friend(Client *client, const char *to) {
    int exists;
    int already_friends;
    int already_requested;

    if (!valid_token(to)) {
        send_notice(client, "好友账号无效。");
        return;
    }
    if (strcmp(to, client->user) == 0) {
        send_notice(client, "不能添加自己为好友。");
        return;
    }

    pthread_mutex_lock(&g_store_mutex);
    exists = user_exists_locked(to);
    already_friends = exists ? are_friends_locked(client->user, to) : 0;
    already_requested = exists ? friend_request_exists_locked(client->user, to) : 0;
    if (exists && !already_friends && !already_requested) {
        add_friend_request_locked(client->user, to);
    }
    pthread_mutex_unlock(&g_store_mutex);

    if (!exists) {
        send_notice(client, "该账号不存在，无法添加好友。");
    } else if (already_friends) {
        send_notice(client, "%s 已经是你的好友。", to);
    } else if (already_requested) {
        send_notice(client, "已经向 %s 发送过好友申请。", to);
    } else {
        /* 在线则实时通知 */
        char line[128];
        snprintf(line, sizeof(line), "FRIEND_REQUEST\t%s", client->user);
        notify_online_line(to, line);
        send_notice(client, "好友申请已发送给 %s，等待对方同意。", to);
    }
}

/* 同意好友申请：删除申请记录 → 建立双向好友关系 → 刷新双方状态 */
static void accept_friend(Client *client, const char *from) {
    if (!valid_token(from)) {
        send_notice(client, "好友申请来源无效。");
        return;
    }
    if (!remove_friend_request(from, client->user)) {
        send_notice(client, "没有找到来自 %s 的好友申请。", from);
        return;
    }
    add_friend_pair(from, client->user);
    send_notice(client, "已同意 %s 的好友申请。", from);
    notify_online_notice(from, "%s 已同意你的好友申请。", client->user);
    refresh_user_state(client->user);
    refresh_user_state(from);
}

/* 拒绝好友申请：删除申请记录 → 通知双方 */
static void reject_friend(Client *client, const char *from) {
    if (!valid_token(from)) {
        send_notice(client, "好友申请来源无效。");
        return;
    }
    if (remove_friend_request(from, client->user)) {
        send_notice(client, "已拒绝 %s 的好友申请。", from);
        notify_online_notice(from, "%s 拒绝了你的好友申请。", client->user);
    } else {
        send_notice(client, "没有找到来自 %s 的好友申请。", from);
    }
}

/* 删除好友：删除双向好友关系 → 刷新双方状态 */
static void delete_friend(Client *client, const char *who) {
    int friends;
    if (!valid_token(who)) {
        send_notice(client, "好友账号无效。");
        return;
    }
    pthread_mutex_lock(&g_store_mutex);
    friends = are_friends_locked(client->user, who);
    pthread_mutex_unlock(&g_store_mutex);
    if (!friends) {
        send_notice(client, "%s 不是你的好友。", who);
        return;
    }
    if (remove_friend_pair(client->user, who)) {
        sendf_client(client, "OK\tDELETE_FRIEND\t%s", who);
        send_notice(client, "已删除好友 %s。", who);
        refresh_user_state(client->user);
        notify_online_notice(who, "%s 已将你从好友列表删除。", client->user);
        refresh_user_state(who);
    } else {
        send_notice(client, "删除好友失败。");
    }
}

/* 创建群：群名不能与已有群冲突，创建者自动成为第一个成员 */
static void create_group(Client *client, const char *group) {
    int exists;
    int member;
    if (!valid_token(group)) {
        send_notice(client, "群名无效，不能包含空白控制字符，长度需小于 %d。", QQ_MAX_NAME);
        return;
    }
    if (strcmp(group, "lobby") == 0) {
        send_notice(client, "公共群 lobby 已移除，请创建自己的群。");
        return;
    }
    pthread_mutex_lock(&g_store_mutex);
    exists = group_exists_locked(group);
    member = exists ? is_group_member_locked(group, client->user) : 0;
    pthread_mutex_unlock(&g_store_mutex);
    if (exists && !member) {
        send_notice(client, "群 %s 已存在，需要群成员邀请后才能加入。", group);
        return;
    }
    join_group(group, client->user);  /* 创建者自动加入 */
    sendf_client(client, "OK\tCREATE_GROUP\t%s", group);
    send_group_list(client);
}

/* 邀请入群：需要邀请者是群成员、目标用户存在且不在群内 */
static void invite_group(Client *client, const char *group, const char *to) {
    int exists;
    int inviter_member;
    int target_member;
    int already_invited;
    if (!valid_token(group) || !valid_token(to)) {
        send_notice(client, "群名或账号无效。");
        return;
    }
    if (strcmp(to, client->user) == 0) {
        send_notice(client, "不能邀请自己进群。");
        return;
    }
    pthread_mutex_lock(&g_store_mutex);
    exists = user_exists_locked(to);
    inviter_member = group_exists_locked(group) && is_group_member_locked(group, client->user);
    target_member = inviter_member ? is_group_member_locked(group, to) : 0;
    already_invited = inviter_member ? group_invite_exists_locked(group, client->user, to) : 0;
    if (exists && inviter_member && !target_member && !already_invited) {
        add_group_invite_locked(group, client->user, to);
    }
    pthread_mutex_unlock(&g_store_mutex);

    if (!exists) {
        send_notice(client, "该账号不存在，无法邀请进群。");
    } else if (!inviter_member) {
        send_notice(client, "你还不是群 %s 的成员，不能邀请别人。", group);
    } else if (target_member) {
        send_notice(client, "%s 已经在群 %s 中。", to, group);
    } else if (already_invited) {
        send_notice(client, "已经邀请过 %s 加入群 %s。", to, group);
    } else {
        char line[256];
        snprintf(line, sizeof(line), "GROUP_INVITE\t%s\t%s", group, client->user);
        notify_online_line(to, line);
        send_notice(client, "已邀请 %s 加入群 %s，等待对方同意。", to, group);
    }
}

/* 同意群邀请：删除邀请记录 → 加入群 → 通知邀请者 */
static void accept_group(Client *client, const char *group, const char *from) {
    if (!valid_token(group) || !valid_token(from)) {
        send_notice(client, "群邀请参数无效。");
        return;
    }
    if (strcmp(group, "lobby") == 0) {
        remove_group_invite(group, from, client->user);
        send_notice(client, "公共群 lobby 已移除，请加入别人创建的群。");
        return;
    }
    if (!remove_group_invite(group, from, client->user)) {
        send_notice(client, "没有找到来自 %s 的群 %s 邀请。", from, group);
        return;
    }
    join_group(group, client->user);
    sendf_client(client, "OK\tACCEPT_GROUP\t%s", group);
    send_notice(client, "已加入群 %s。", group);
    send_group_list(client);
    notify_online_notice(from, "%s 已接受你的邀请，加入群 %s。", client->user, group);
}

/* 拒绝群邀请 */
static void reject_group(Client *client, const char *group, const char *from) {
    if (!valid_token(group) || !valid_token(from)) {
        send_notice(client, "群邀请参数无效。");
        return;
    }
    if (remove_group_invite(group, from, client->user)) {
        send_notice(client, "已拒绝加入群 %s。", group);
        notify_online_notice(from, "%s 拒绝加入群 %s。", client->user, group);
    } else {
        send_notice(client, "没有找到来自 %s 的群 %s 邀请。", from, group);
    }
}

/* 退群：从群成员中移除自己 */
static void leave_group(Client *client, const char *group) {
    int member;
    if (!valid_token(group) || strcmp(group, "lobby") == 0) {
        send_notice(client, "群名无效。");
        return;
    }
    pthread_mutex_lock(&g_store_mutex);
    member = is_group_member_locked(group, client->user);
    pthread_mutex_unlock(&g_store_mutex);
    if (!member) {
        send_notice(client, "你还不是群 %s 的成员。", group);
        return;
    }
    if (remove_group_member(group, client->user)) {
        sendf_client(client, "OK\tLEAVE_GROUP\t%s", group);
        send_notice(client, "已退出群 %s。", group);
        send_group_list(client);
    } else {
        send_notice(client, "退群失败。");
    }
}

/*
 * ======== 客户端连接处理（核心入口） ========
 *
 * handle_client — 处理一个客户端的完整生命周期
 *
 * 生命周期流程：
 *   1. 接收第一条命令（必须是 LOGIN 或 REGISTER）
 *   2. 验证用户身份（检查 users.tsv 中的账号密码）
 *   3. 登录成功后依次发送：好友列表、群列表、待处理申请、离线消息
 *   4. 进入命令分发循环，解析并处理后续每条命令
 *   5. 客户端发送 QUIT 或连接断开后执行清理
 *
 * 这是服务端处理每个客户端连接的主函数，被 worker 线程调用。
 * 一个 worker 线程同一时间只服务一个客户端，直到客户端断开。
 */
static void handle_client(Job job) {
    char line[QQ_MAX_LINE];
    Client *client = NULL;

    /* ---- 第一步：读取并验证登录/注册命令 ---- */
    if (qq_recv_line(job.fd, line, sizeof(line)) <= 0) {
        close(job.fd);
        return;
    }

    qq_trim_newline(line);
    char *cmd = strtok(line, "\t");
    /* 第一条命令必须是 LOGIN 或 REGISTER，不是则断开 */
    if (!cmd || (strcmp(cmd, "LOGIN") != 0 && strcmp(cmd, "REGISTER") != 0)) {
        qq_send_line(job.fd, "ERR\tfirst command must be LOGIN or REGISTER");
        close(job.fd);
        return;
    }

    /* 解析命令参数：LOGIN/REGISTER + 用户名 + 显示名(转义) + P2P端口 + 密码(转义) */
    char *user = strtok(NULL, "\t");
    char *display_enc = strtok(NULL, "\t");
    char *peer_s = strtok(NULL, "\t");
    char *pass_enc = strtok(NULL, "\t");
    char *display = qq_unescape(display_enc ? display_enc : user);
    char *password = qq_unescape(pass_enc ? pass_enc : "");
    char stored_display[QQ_MAX_NAME] = "";
    char stored_password[128] = "";
    int peer_port = peer_s ? atoi(peer_s) : 0;

    if (!valid_token(user)) {
        qq_send_line(job.fd, "ERR\tinvalid user");
        free(display);
        free(password);
        close(job.fd);
        return;
    }

    /* ---- 第二步：注册或登录验证 ---- */
    pthread_mutex_lock(&g_store_mutex);
    if (strcmp(cmd, "REGISTER") == 0) {
        /* 注册：检查用户是否已存在，不存在则创建 */
        if (user_exists_locked(user)) {
            pthread_mutex_unlock(&g_store_mutex);
            qq_send_line(job.fd, "ERR\taccount already exists");
            free(display);
            free(password);
            close(job.fd);
            return;
        }
        if (!create_user_locked(user, display && *display ? display : user, password ? password : "")) {
            pthread_mutex_unlock(&g_store_mutex);
            qq_send_line(job.fd, "ERR\tcannot create account");
            free(display);
            free(password);
            close(job.fd);
            return;
        }
        snprintf(stored_display, sizeof(stored_display), "%s", display && *display ? display : user);
    } else {
        /* 登录：检查用户是否存在、密码是否正确 */
        if (!read_user_locked(user, stored_display, sizeof(stored_display),
                              stored_password, sizeof(stored_password))) {
            pthread_mutex_unlock(&g_store_mutex);
            qq_send_line(job.fd, "ERR\taccount not found, please register first");
            free(display);
            free(password);
            close(job.fd);
            return;
        }
        if (stored_password[0] && strcmp(stored_password, password ? password : "") != 0) {
            pthread_mutex_unlock(&g_store_mutex);
            qq_send_line(job.fd, "ERR\tpassword incorrect");
            free(display);
            free(password);
            close(job.fd);
            return;
        }
    }
    pthread_mutex_unlock(&g_store_mutex);

    /* ---- 第三步：注册到在线客户端表 ---- */
    if (attach_client(&client, job.fd, job.ip, user,
                      stored_display[0] ? stored_display : (display ? display : user),
                      peer_port) < 0) {
        qq_send_line(job.fd, "ERR\tuser already online or server full");
        free(display);
        free(password);
        close(job.fd);
        return;
    }
    free(display);
    free(password);

    /* ---- 第四步：发送登录成功响应和初始数据 ---- */
    sendf_client(client, "OK\t%s\t%s", cmd, client->user);
    send_counts(client);              /* 好友统计 */
    send_friend_list(client);         /* 好友列表（含在线状态） */
    send_group_list(client);          /* 群列表 */
    deliver_pending_requests(client); /* 待处理的好友申请和群邀请 */
    deliver_offline(client);          /* 离线消息 */
    broadcast_counts();               /* 更新所有客户端的在线人数 */
    refresh_friends_of_user(client->user); /* 刷新好友侧的在线/离线状态 */
    pthread_mutex_lock(&g_clients_mutex);
    stats_login(online_count_locked());
    pthread_mutex_unlock(&g_clients_mutex);
    server_log("%s logged in from %s p2p=%d", client->user, client->ip, client->peer_port);

    /* ---- 第五步：命令分发循环 ---- */
    while (g_running && qq_recv_line(client->fd, line, sizeof(line)) > 0) {
        char *parts[5] = {0};  /* 最多 5 个字段 */
        char *p;
        int i = 0;
        qq_trim_newline(line);
        /* 按 TAB 分隔解析命令字段 */
        p = strtok(line, "\t");
        while (p && i < 5) {
            parts[i++] = p;
            p = strtok(NULL, "\t");
        }
        if (!parts[0]) {
            continue;  /* 空行忽略 */
        }

        /* ---- 命令分发 ---- */

        if (strcmp(parts[0], "QUIT") == 0) {
            /* 客户端主动断开 */
            send_client(client, "BYE");
            break;
        } else if (strcmp(parts[0], "LIST") == 0) {
            /* 刷新好友列表和群列表 */
            send_counts(client);
            send_friend_list(client);
            send_group_list(client);
        } else if (strcmp(parts[0], "ADD_FRIEND") == 0 && parts[1]) {
            request_friend(client, parts[1]);
        } else if (strcmp(parts[0], "ACCEPT_FRIEND") == 0 && parts[1]) {
            accept_friend(client, parts[1]);
        } else if (strcmp(parts[0], "REJECT_FRIEND") == 0 && parts[1]) {
            reject_friend(client, parts[1]);
        } else if (strcmp(parts[0], "DELETE_FRIEND") == 0 && parts[1]) {
            delete_friend(client, parts[1]);
        } else if (strcmp(parts[0], "MSGP") == 0 && parts[1] && parts[2]) {
            /* 私聊消息：解码后路由 */
            char *text = qq_unescape(parts[2]);
            route_private(client, parts[1], text ? text : parts[2]);
            stats_message("P");
            free(text);
        } else if (strcmp(parts[0], "CREATE_GROUP") == 0 && parts[1]) {
            create_group(client, parts[1]);
        } else if (strcmp(parts[0], "INVITE_GROUP") == 0 && parts[1] && parts[2]) {
            invite_group(client, parts[1], parts[2]);
        } else if (strcmp(parts[0], "ACCEPT_GROUP") == 0 && parts[1] && parts[2]) {
            accept_group(client, parts[1], parts[2]);
        } else if (strcmp(parts[0], "REJECT_GROUP") == 0 && parts[1] && parts[2]) {
            reject_group(client, parts[1], parts[2]);
        } else if (strcmp(parts[0], "LEAVE_GROUP") == 0 && parts[1]) {
            leave_group(client, parts[1]);
        } else if (strcmp(parts[0], "JOIN") == 0 && parts[1]) {
            /* 旧版兼容：JOIN 命令已废弃 */
            send_notice(client, "没有默认公共群，请自己建群或接受群邀请。");
        } else if (strcmp(parts[0], "MSGG") == 0 && parts[1] && parts[2]) {
            /* 群聊消息：先校验发送者是群成员，再路由 */
            char *text = qq_unescape(parts[2]);
            pthread_mutex_lock(&g_store_mutex);
            int member = is_group_member_locked(parts[1], client->user);
            pthread_mutex_unlock(&g_store_mutex);
            if (member) {
                route_group(client, parts[1], text ? text : parts[2]);
                stats_message("G");
            } else {
                send_notice(client, "你还不是群 %s 的成员，无法发送群消息。", parts[1]);
            }
            free(text);
        } else if (strcmp(parts[0], "HISTORY") == 0 && parts[1] && parts[2]) {
            /* 历史消息查询 */
            send_history(client, parts[1], parts[2], parts[3] ? atoi(parts[3]) : 50);
        } else if (strcmp(parts[0], "SEARCH") == 0 && parts[1]) {
            /* 关键字检索 */
            char *kw = qq_unescape(parts[1]);
            send_search(client, kw ? kw : parts[1]);
            free(kw);
        } else if (strcmp(parts[0], "PEER_INFO") == 0 && parts[1]) {
            /* 查询 P2P 对端信息 */
            send_peer_info(client, parts[1]);
        } else if (strcmp(parts[0], "FILE_NOTICE") == 0 && parts[1] && parts[2] && parts[3]) {
            /* 文件传输通知 */
            char *filename = qq_unescape(parts[2]);
            route_file_notice(client, parts[1], filename ? filename : parts[2], parts[3]);
            free(filename);
        } else {
            send_client(client, "ERR\tunknown or malformed command");
        }
    }

    /* ---- 第六步：清理 ---- */
    char disconnected_user[QQ_MAX_NAME];
    snprintf(disconnected_user, sizeof(disconnected_user), "%s", client->user);
    server_log("%s disconnected", client->user);
    close(client->fd);           /* 关闭 socket */
    detach_client(client);       /* 从在线表中移除 */
    broadcast_counts();          /* 更新所有客户端的在线人数 */
    refresh_friends_of_user(disconnected_user); /* 刷新好友侧的在线/离线状态 */
    pthread_mutex_lock(&g_clients_mutex);
    stats_online(online_count_locked());
    pthread_mutex_unlock(&g_clients_mutex);
}

/* ======== Worker 线程入口 ======== */

/*
 * worker_main — worker 线程的主函数
 *
 * 每个 worker 线程循环从任务队列取出客户端连接，
 * 调用 handle_client() 处理其完整生命周期。
 * 服务端启动时一次性创建，运行期间复用。
 */
static void *worker_main(void *arg) {
    (void)arg;
    while (g_running) {
        Job job;
        if (queue_pop(&job)) {
            /* 处理一个客户端连接（阻塞直到客户端断开） */
            handle_client(job);
        }
    }
    return NULL;
}

/* ======== main 函数 ======== */

/*
 * main — 服务端入口
 *
 * 启动流程：
 *   1. 解析命令行参数（端口号、worker 线程数）
 *   2. 注册信号处理函数（SIGINT/SIGTERM → 优雅退出）
 *   3. 初始化数据目录和共享内存
 *   4. 创建监听 socket
 *   5. 创建 worker 线程池
 *   6. 进入 accept 循环：接收连接 → 压入任务队列
 *   7. 收到退出信号后：关闭监听 socket → 等待所有 worker 退出 → 清理资源
 *
 * 用法：./qq_server [端口号] [worker线程数]
 * 示例：./qq_server 9090 8
 */
int main(int argc, char **argv) {
    int port = argc > 1 ? atoi(argv[1]) : QQ_DEFAULT_PORT;
    int workers = argc > 2 ? atoi(argv[2]) : DEFAULT_WORKERS;
    int server_fd;
    int opt = 1;
    struct sockaddr_in addr;
    pthread_t *threads;
    int i;

    /* worker 线程数限制：最少 1 个，最多 64 个 */
    if (workers <= 0 || workers > 64) {
        workers = DEFAULT_WORKERS;
    }

    /* 注册信号处理：Ctrl+C 或 kill 时触发优雅退出 */
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN);    /* 忽略 SIGPIPE，防止写已关闭的 socket 时进程崩溃 */

    ensure_storage();            /* 确保 data/ 目录存在 */
    init_shared_stats();         /* 初始化共享内存统计 */

    /* 创建 TCP 监听 socket */
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        die("socket");
    }
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));  /* 允许端口复用 */
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);  /* 监听所有网卡 */
    addr.sin_port = htons((uint16_t)port);
    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        die("bind");
    }
    if (listen(server_fd, 128) < 0) {  /* backlog = 128 */
        die("listen");
    }

    /* 创建 worker 线程池 */
    threads = calloc((size_t)workers, sizeof(*threads));
    if (!threads) {
        die("calloc");
    }
    for (i = 0; i < workers; i++) {
        pthread_create(&threads[i], NULL, worker_main, NULL);
    }

    server_log("qq_server listening on 0.0.0.0:%d with %d worker threads", port, workers);

    /* 主循环：accept 新连接 → 投递给线程池 */
    while (g_running) {
        struct sockaddr_in peer;
        socklen_t len = sizeof(peer);
        char ip[INET_ADDRSTRLEN] = "0.0.0.0";
        int fd = accept(server_fd, (struct sockaddr *)&peer, &len);
        if (fd < 0) {
            if (errno == EINTR) {
                continue;  /* 被信号中断，检查 g_running 后重试 */
            }
            perror("accept");
            break;
        }
        inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip));  /* 二进制 IP → 字符串 */
        /* 主线程不直接处理客户端，只负责分发 */
        queue_push(fd, ip);
    }

    /* ---- 优雅退出 ---- */
    close(server_fd);  /* 关闭监听 socket，不再接受新连接 */

    /* 广播唤醒所有阻塞的 worker 线程，告知服务端正在关闭 */
    pthread_mutex_lock(&g_queue.mutex);
    pthread_cond_broadcast(&g_queue.has_job);
    pthread_mutex_unlock(&g_queue.mutex);

    /* 等待所有 worker 线程结束 */
    for (i = 0; i < workers; i++) {
        pthread_join(threads[i], NULL);
    }
    free(threads);

    return 0;
}
