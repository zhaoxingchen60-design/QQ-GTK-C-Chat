/*
 * client_gtk4.c — QQ 聊天系统 GTK4 图形客户端
 *
 * ======== 客户端整体架构 ========
 *
 * 客户端由四个核心部分组成：
 *
 *   1. GTK 主线程
 *      负责登录窗口、主界面、联系人列表、聊天气泡、文件记录等所有 UI 绘制。
 *      GTK 控件不是线程安全的，因此任何后台线程都不能直接改控件。
 *
 *   2. 控制长连接
 *      登录成功后，客户端与服务端保持一条 TCP 长连接 app->sockfd。
 *      私聊、群聊、好友申请、群邀请、历史、搜索、P2P 地址查询等控制命令都复用这条连接。
 *
 *   3. 服务端接收线程 receiver_main()
 *      独立线程持续读取服务端协议行，并根据命令类型分发。
 *      需要更新界面时，通过 g_idle_add() 把任务投递回 GTK 主线程执行。
 *
 *   4. P2P 文件传输线程
 *      file_listener_main() 常驻监听随机端口，用于接收其它客户端直连发送的文件。
 *      file_send_main() 每发送一个文件启动一个独立线程，避免大文件传输阻塞界面。
 *
 * ======== 协议分层 ========
 *
 * client_gtk4.c 不直接处理底层分包、短写、换行帧等细节，
 * 这些都由 protocol.c 中的 qq_send_line()、qq_recv_line()、qq_escape() 完成。
 * 本文件只关心业务命令，例如 MSGP、MSGG、HISTORY、PEER_INFO、FILE_NOTICE。
 *
 * ======== 线程安全要点 ========
 *
 * - app->send_mutex：保护 app->sockfd，防止多个线程同时发送协议行造成串包。
 * - app->peer_mutex + app->peer_cond：文件发送线程等待 receiver_main() 收到 PEER 响应。
 * - g_idle_add()：后台线程与 GTK 主线程之间的安全桥梁。
 */

#include "protocol.h"

#include <arpa/inet.h>
#include <errno.h>
#include <gtk/gtk.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <locale.h>
#include <time.h>
#include <unistd.h>

#define DOWNLOAD_DIR "downloads"

/* 主界面左侧导航的四个页面编号。 */
enum {
    TAB_MESSAGES,
    TAB_FRIENDS,
    TAB_GROUPS,
    TAB_FILES
};

/* 好友列表项：服务端返回好友账号和是否在线。 */
typedef struct {
    char user[QQ_MAX_NAME];
    int online;
} FriendInfo;

/* 群列表项：当前用户加入的群名。 */
typedef struct {
    char name[QQ_MAX_NAME];
} GroupInfo;

/* 文件记录：用于“文件”页展示收发记录。 */
typedef struct {
    char name[256];
    char peer[QQ_MAX_NAME];
    char path[1024];
    int incoming;
} FileInfo;

/* 待处理请求：好友申请和群邀请共用这一个结构。 */
typedef struct {
    char type[16];
    char from[QQ_MAX_NAME];
    char group[QQ_MAX_NAME];
} RequestInfo;

/*
 * 客户端全局状态：
 * - GTK 控件指针集中保存，方便不同回调更新界面。
 * - sockfd 是与服务器的控制长连接，承载私聊/群聊/历史/搜索/好友等命令。
 * - file_listen_fd 是 P2P 文件接收端口，文件数据不经过服务器。
 * - friends/groups/files/requests 是界面侧缓存，服务端推送列表后刷新。
 * - peer_* 字段保存最近一次 PEER_INFO 查询结果，供文件发送线程等待使用。
 */
typedef struct {
    /* GTK 应用、窗口和主要控件。 */
    GtkApplication *gtk_app;
    GtkWidget *login_window;
    GtkWidget *main_window;
    GtkWidget *user_entry;
    GtkWidget *pass_entry;
    GtkWidget *login_error;
    GtkWidget *sidebar_title;
    GtkWidget *profile_sub;
    GtkWidget *friend_entry;
    GtkWidget *friend_tools;
    GtkWidget *friend_action_button;
    GtkWidget *group_invite_entry;
    GtkWidget *group_invite_button;
    GtkWidget *delete_friend_button;
    GtkWidget *leave_group_button;
    GtkWidget *list_box;
    GtkWidget *chat_title;
    GtkWidget *chat_subtitle;
    GtkWidget *chat_box;
    GtkWidget *chat_scroll;
    GtkWidget *message_entry;
    GtkWidget *search_entry;
    GtkWidget *search_tools;
    GtkWidget *search_button;
    GtkWidget *nav_buttons[4];

    /* 网络连接和线程生命周期。 */
    int sockfd;
    int file_listen_fd;
    int file_port;
    int shutting_down;
    int recv_started;
    int file_thread_started;

    /* 当前 UI 状态。current_mode: 0=私聊，1=群聊。 */
    int active_tab;
    int friend_count;
    int online_friend_count;
    int current_mode;
    int history_rows;
    int history_allow_fallback;
    int history_all_mode;
    char current_target[QQ_MAX_NAME];
    char host[128];
    int port;
    char user[QQ_MAX_NAME];
    char password[128];
    char download_dir[1024];

    /* GTK/GLib 动态数组，元素由 g_free 释放。 */
    GPtrArray *friends;
    GPtrArray *groups;
    GPtrArray *files;
    GPtrArray *requests;

    /* 后台线程和跨线程同步对象。 */
    pthread_t recv_thread;
    pthread_t file_thread;
    pthread_mutex_t send_mutex;
    pthread_mutex_t peer_mutex;
    pthread_cond_t peer_cond;

    /* PEER_INFO 查询结果：receiver_main 收到 PEER 后唤醒等待的发送线程。 */
    int peer_ready;
    int peer_online;
    int peer_port;
    char peer_user[QQ_MAX_NAME];
    char peer_ip[64];
} AppState;

typedef struct {
    AppState *app;
    int kind;
    int outgoing;
    char actor[QQ_MAX_NAME];
    char text[4096];
} UiMessage;

/*
 * Ui* 结构用于跨线程投递 UI 更新任务。
 *
 * receiver_main()、file_listener_main()、file_send_main() 都是 pthread 线程，
 * 不能直接调用 gtk_box_append()、gtk_label_set_text() 等 GTK API 修改控件。
 * 因此后台线程只构造 UiMessage/UiFriend/UiGroup 等小对象，
 * 再通过 g_idle_add() 交给 GTK 主循环在主线程中执行。
 */
typedef struct {
    AppState *app;
    FriendInfo *friend_info;
} UiFriend;

typedef struct {
    AppState *app;
    GroupInfo *group_info;
} UiGroup;

typedef struct {
    AppState *app;
    FileInfo *file_info;
} UiFile;

typedef struct {
    AppState *app;
    GtkWidget *row;
    char type[16];
    char from[QQ_MAX_NAME];
    char group[QQ_MAX_NAME];
    int accept;
} RequestAction;

typedef struct {
    AppState *app;
    char type[16];
    char from[QQ_MAX_NAME];
    char group[QQ_MAX_NAME];
} UiRequest;

typedef struct {
    AppState *app;
    int mode;
    int tab;
    char target[QQ_MAX_NAME];
} UiChoose;

typedef struct {
    AppState *app;
    char user[QQ_MAX_NAME];
    char ip[64];
    int port;
    int online;
} PeerInfo;

typedef struct {
    AppState *app;
    int friends;
    int online_friends;
} UiCounts;

typedef struct {
    AppState *app;
    char to[QQ_MAX_NAME];
    char path[1024];
} FileSendJob;

static void *file_send_main(void *arg);
static void render_sidebar(AppState *app);
static void set_tab(AppState *app, int tab);
static GtkWidget *tool_button(const char *label, const char *klass);
static void menu_action_clicked(GtkButton *button, gpointer data);
static int has_group(AppState *app, const char *name);

/* 历史数据里可能有 G/MSGG/GROUP 等格式，这里统一识别为群聊。 */
static int client_kind_is_group(const char *kind) {
    return kind &&
           (strcmp(kind, "G") == 0 ||
            strcmp(kind, "MSGG") == 0 ||
            strcmp(kind, "GROUP") == 0 ||
            strcmp(kind, "group") == 0);
}

/* 连接服务器，建立后续所有控制消息复用的 TCP 长连接。 */
static int connect_to_server(const char *host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;
    if (fd < 0) {
        return -1;
    }
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1 ||
        connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

/* 多线程场景下发送命令要加锁，避免接收线程/文件线程同时写 socket 串包。 */
static int send_cmd(AppState *app, const char *line) {
    int rc;
    pthread_mutex_lock(&app->send_mutex);
    rc = qq_send_line(app->sockfd, line);
    pthread_mutex_unlock(&app->send_mutex);
    return rc;
}

/*
 * send_cmdf — printf 风格的协议命令发送封装
 *
 * 上层回调只需要写 send_cmdf(app, "MSGP\t%s\t%s", to, text)，
 * 具体的格式化、互斥发送、行尾换行由 send_cmdf()/send_cmd()/protocol.c 统一处理。
 */
static int send_cmdf(AppState *app, const char *fmt, ...) {
    char line[QQ_MAX_LINE];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(line, sizeof(line), fmt, ap);
    va_end(ap);
    return send_cmd(app, line);
}

/*
 * avatar_draw — 根据用户名绘制圆形文字头像
 *
 * 这里没有依赖外部图片，而是用用户名 hash 生成稳定颜色，再画首字符。
 * 好处是演示时不需要准备头像资源，并且同一个用户每次显示颜色一致。
 */
static void avatar_draw(GtkDrawingArea *area, cairo_t *cr, int width, int height, gpointer data) {
    const char *name = (const char *)data;
    guint hash = g_str_hash(name ? name : "u");
    double r = 0.15 + ((hash >> 0) & 31) / 90.0;
    double g = 0.20 + ((hash >> 5) & 31) / 90.0;
    double b = 0.38 + ((hash >> 10) & 31) / 90.0;
    double radius = (width < height ? width : height) / 2.0 - 1.5;
    (void)area;

    cairo_set_source_rgb(cr, r, g, b);
    cairo_arc(cr, width / 2.0, height / 2.0, radius, 0, 6.28318);
    cairo_fill(cr);
    cairo_set_source_rgb(cr, 1, 1, 1);
    cairo_select_font_face(cr, "Sans", CAIRO_FONT_SLANT_NORMAL, CAIRO_FONT_WEIGHT_BOLD);
    cairo_set_font_size(cr, width > 70 ? 30 : 15);
    cairo_move_to(cr, width / 2.0 - (width > 70 ? 10 : 5), height / 2.0 + (width > 70 ? 10 : 5));
    cairo_show_text(cr, name && *name ? name : "?");
}

/* qq_avatar_draw — 登录页 QQ 风格头像，使用 Cairo 基础图形直接绘制。 */
static void qq_avatar_draw(GtkDrawingArea *area, cairo_t *cr, int width, int height, gpointer data) {
    double radius = (width < height ? width : height) / 2.0 - 3.0;
    double cx = width / 2.0;
    double cy = height / 2.0;
    (void)area;
    (void)data;

    cairo_set_source_rgb(cr, 0.92, 0.91, 0.84);
    cairo_arc(cr, cx, cy, radius, 0, 6.28318);
    cairo_fill(cr);
    cairo_set_source_rgb(cr, 1, 1, 1);
    cairo_set_line_width(cr, 3);
    cairo_arc(cr, cx, cy, radius - 1.5, 0, 6.28318);
    cairo_stroke(cr);
    cairo_set_source_rgb(cr, 0.08, 0.08, 0.10);
    cairo_arc(cr, cx, cy + 10, radius * 0.46, 0, 6.28318);
    cairo_fill(cr);
    cairo_arc(cr, cx, cy - 15, radius * 0.34, 0, 6.28318);
    cairo_fill(cr);
    cairo_set_source_rgb(cr, 0.96, 0.96, 0.94);
    cairo_arc(cr, cx, cy + 13, radius * 0.26, 0, 6.28318);
    cairo_fill(cr);
    cairo_set_source_rgb(cr, 1, 1, 1);
    cairo_arc(cr, cx - 11, cy - 20, 5, 0, 6.28318);
    cairo_arc(cr, cx + 11, cy - 20, 5, 0, 6.28318);
    cairo_fill(cr);
    cairo_set_source_rgb(cr, 0.05, 0.05, 0.06);
    cairo_arc(cr, cx - 11, cy - 20, 2.5, 0, 6.28318);
    cairo_arc(cr, cx + 11, cy - 20, 2.5, 0, 6.28318);
    cairo_fill(cr);
    cairo_set_source_rgb(cr, 0.95, 0.70, 0.16);
    cairo_move_to(cr, cx - 7, cy - 9);
    cairo_line_to(cr, cx + 7, cy - 9);
    cairo_line_to(cr, cx, cy - 2);
    cairo_close_path(cr);
    cairo_fill(cr);
}

static GtkWidget *make_avatar(const char *name, int size) {
    GtkWidget *area = gtk_drawing_area_new();
    gtk_widget_set_size_request(area, size, size);
    gtk_drawing_area_set_draw_func(GTK_DRAWING_AREA(area), avatar_draw, g_strdup(name ? name : "?"), g_free);
    return area;
}

static char *clock_label(long ts) {
    time_t t = (time_t)ts;
    struct tm tmv;
    char buf[32];
    localtime_r(&t, &tmv);
    strftime(buf, sizeof(buf), "%H:%M:%S", &tmv);
    return g_strdup(buf);
}

static gboolean scroll_bottom(gpointer data) {
    AppState *app = (AppState *)data;
    GtkAdjustment *adj = gtk_scrolled_window_get_vadjustment(GTK_SCROLLED_WINDOW(app->chat_scroll));
    gtk_adjustment_set_value(adj, gtk_adjustment_get_upper(adj) - gtk_adjustment_get_page_size(adj));
    return G_SOURCE_REMOVE;
}

/*
 * ui_add_message — 在 GTK 主线程中追加一条聊天气泡或系统提示
 *
 * kind=0 表示系统提示，居中显示；kind=1 表示聊天消息。
 * outgoing 决定气泡左右方向：自己发送的靠右，对方消息靠左。
 */
static gboolean ui_add_message(gpointer data) {
    UiMessage *m = (UiMessage *)data;
    GtkWidget *row = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 10);
    GtkWidget *label = gtk_label_new(m->text);

    gtk_label_set_wrap(GTK_LABEL(label), TRUE);
    gtk_label_set_wrap_mode(GTK_LABEL(label), PANGO_WRAP_WORD_CHAR);
    gtk_label_set_max_width_chars(GTK_LABEL(label), m->kind ? 50 : 78);
    gtk_widget_add_css_class(label, m->kind == 0 ? "sys-badge" : (m->outgoing ? "bubble-out" : "bubble-in"));
    gtk_widget_set_margin_top(row, 7);
    gtk_widget_set_margin_bottom(row, 7);
    gtk_widget_set_margin_start(row, 24);
    gtk_widget_set_margin_end(row, 24);

    if (m->kind == 0) {
        gtk_widget_set_halign(label, GTK_ALIGN_CENTER);
        gtk_box_append(GTK_BOX(row), label);
    } else if (m->outgoing) {
        GtkWidget *avatar = make_avatar(m->app->user, 36);
        gtk_widget_set_halign(row, GTK_ALIGN_END);
        gtk_box_append(GTK_BOX(row), label);
        gtk_box_append(GTK_BOX(row), avatar);
    } else {
        GtkWidget *avatar = make_avatar(m->actor, 36);
        gtk_widget_set_halign(row, GTK_ALIGN_START);
        gtk_box_append(GTK_BOX(row), avatar);
        gtk_box_append(GTK_BOX(row), label);
    }

    gtk_box_append(GTK_BOX(m->app->chat_box), row);
    g_idle_add(scroll_bottom, m->app);
    g_free(m);
    return G_SOURCE_REMOVE;
}

/* append_notice — 后台线程安全的系统提示入口，内部用 g_idle_add 投递 UI 更新。 */
static void append_notice(AppState *app, const char *fmt, ...) {
    UiMessage *m = g_new0(UiMessage, 1);
    va_list ap;
    m->app = app;
    m->kind = 0;
    va_start(ap, fmt);
    vsnprintf(m->text, sizeof(m->text), fmt, ap);
    va_end(ap);
    g_idle_add(ui_add_message, m);
}

/* append_chat — 后台线程安全的聊天消息入口，统一拼接标题和正文后投递给主线程。 */
static void append_chat(AppState *app, int outgoing, const char *actor, const char *title, const char *text) {
    UiMessage *m = g_new0(UiMessage, 1);
    m->app = app;
    m->kind = 1;
    m->outgoing = outgoing;
    snprintf(m->actor, sizeof(m->actor), "%s", actor ? actor : "");
    snprintf(m->text, sizeof(m->text), "%s\n%s", title ? title : "", text ? text : "");
    g_idle_add(ui_add_message, m);
}

static gboolean ui_add_file(gpointer data) {
    UiFile *ui = (UiFile *)data;
    g_ptr_array_add(ui->app->files, ui->file_info);
    if (ui->app->active_tab == TAB_FILES) {
        render_sidebar(ui->app);
    }
    g_free(ui);
    return G_SOURCE_REMOVE;
}

static gboolean ui_clear_groups(gpointer data) {
    AppState *app = (AppState *)data;
    g_ptr_array_set_size(app->groups, 0);
    if (app->active_tab == TAB_GROUPS || app->active_tab == TAB_MESSAGES) {
        render_sidebar(app);
    }
    return G_SOURCE_REMOVE;
}

static gboolean ui_add_group(gpointer data) {
    UiGroup *ui = (UiGroup *)data;
    if (strcmp(ui->group_info->name, "lobby") == 0 || has_group(ui->app, ui->group_info->name)) {
        g_free(ui->group_info);
        g_free(ui);
        return G_SOURCE_REMOVE;
    }
    g_ptr_array_add(ui->app->groups, ui->group_info);
    if (ui->app->active_tab == TAB_GROUPS || ui->app->active_tab == TAB_MESSAGES) {
        render_sidebar(ui->app);
    }
    g_free(ui);
    return G_SOURCE_REMOVE;
}

static int has_group(AppState *app, const char *name) {
    for (guint i = 0; i < app->groups->len; i++) {
        GroupInfo *g = g_ptr_array_index(app->groups, i);
        if (strcmp(g->name, name) == 0) {
            return 1;
        }
    }
    return 0;
}

static gboolean ui_update_counts(gpointer data) {
    UiCounts *ui = (UiCounts *)data;
    char text[160];
    ui->app->friend_count = ui->friends;
    ui->app->online_friend_count = ui->online_friends;
    snprintf(text, sizeof(text), "账号 %s\n好友 %d   在线 %d",
             ui->app->user, ui->friends, ui->online_friends);
    if (ui->app->profile_sub) {
        gtk_label_set_text(GTK_LABEL(ui->app->profile_sub), text);
    }
    g_free(ui);
    return G_SOURCE_REMOVE;
}

static void add_file_record(AppState *app, const char *name, const char *path,
                            const char *peer, int incoming) {
    UiFile *ui = g_new0(UiFile, 1);
    FileInfo *f = g_new0(FileInfo, 1);
    ui->app = app;
    ui->file_info = f;
    snprintf(f->name, sizeof(f->name), "%s", name ? name : "file.bin");
    snprintf(f->path, sizeof(f->path), "%s", path ? path : "");
    snprintf(f->peer, sizeof(f->peer), "%s", peer ? peer : "");
    f->incoming = incoming;
    g_idle_add(ui_add_file, ui);
}

static void request_action_clicked(GtkButton *button, gpointer data) {
    RequestAction *a = (RequestAction *)data;
    (void)button;
    if (strcmp(a->type, "friend") == 0) {
        send_cmdf(a->app, "%s_FRIEND\t%s", a->accept ? "ACCEPT" : "REJECT", a->from);
    } else {
        send_cmdf(a->app, "%s_GROUP\t%s\t%s", a->accept ? "ACCEPT" : "REJECT", a->group, a->from);
    }
    for (guint i = 0; i < a->app->requests->len; i++) {
        RequestInfo *r = g_ptr_array_index(a->app->requests, i);
        if (strcmp(r->type, a->type) == 0 &&
            strcmp(r->from, a->from) == 0 &&
            strcmp(r->group, a->group) == 0) {
            g_ptr_array_remove_index(a->app->requests, i);
            break;
        }
    }
    render_sidebar(a->app);
}

static void request_action_free(gpointer data, GClosure *closure) {
    (void)closure;
    g_free(data);
}

static void add_request_row(AppState *app, const RequestInfo *req) {
    GtkWidget *list_row = gtk_list_box_row_new();
    GtkWidget *row = gtk_box_new(GTK_ORIENTATION_VERTICAL, 8);
    GtkWidget *actions = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);
    GtkWidget *label;
    GtkWidget *accept = tool_button("同意", "primary");
    GtkWidget *reject = tool_button("拒绝", "ghost");
    RequestAction *yes = g_new0(RequestAction, 1);
    RequestAction *no = g_new0(RequestAction, 1);
    char text[256];

    if (strcmp(req->type, "friend") == 0) {
        snprintf(text, sizeof(text), "%s 请求添加你为好友", req->from);
    } else {
        snprintf(text, sizeof(text), "%s 邀请你加入群 %s", req->from, req->group);
    }
    label = gtk_label_new(text);
    gtk_widget_add_css_class(list_row, "request-row");
    gtk_widget_add_css_class(label, "request-title");
    gtk_label_set_xalign(GTK_LABEL(label), 0);
    gtk_label_set_wrap(GTK_LABEL(label), TRUE);
    gtk_widget_set_margin_top(row, 10);
    gtk_widget_set_margin_bottom(row, 10);
    gtk_widget_set_margin_start(row, 14);
    gtk_widget_set_margin_end(row, 14);
    gtk_box_append(GTK_BOX(row), label);
    gtk_box_append(GTK_BOX(actions), accept);
    gtk_box_append(GTK_BOX(actions), reject);
    gtk_box_append(GTK_BOX(row), actions);
    gtk_list_box_row_set_selectable(GTK_LIST_BOX_ROW(list_row), FALSE);
    gtk_list_box_row_set_child(GTK_LIST_BOX_ROW(list_row), row);
    gtk_list_box_append(GTK_LIST_BOX(app->list_box), list_row);

    yes->app = app;
    yes->row = list_row;
    yes->accept = 1;
    snprintf(yes->type, sizeof(yes->type), "%s", req->type);
    snprintf(yes->from, sizeof(yes->from), "%s", req->from);
    snprintf(yes->group, sizeof(yes->group), "%s", req->group);
    *no = *yes;
    no->accept = 0;
    g_signal_connect_data(accept, "clicked", G_CALLBACK(request_action_clicked), yes, request_action_free, 0);
    g_signal_connect_data(reject, "clicked", G_CALLBACK(request_action_clicked), no, request_action_free, 0);
}

static void append_request(AppState *app, const char *type, const char *from, const char *group) {
    RequestInfo *req = g_new0(RequestInfo, 1);
    snprintf(req->type, sizeof(req->type), "%s", type ? type : "");
    snprintf(req->from, sizeof(req->from), "%s", from ? from : "");
    snprintf(req->group, sizeof(req->group), "%s", group ? group : "");
    g_ptr_array_add(app->requests, req);
    set_tab(app, TAB_MESSAGES);
}

static gboolean ui_append_request(gpointer data) {
    UiRequest *r = (UiRequest *)data;
    append_request(r->app, r->type, r->from, r->group);
    g_free(r);
    return G_SOURCE_REMOVE;
}

/*
 * init_download_dir — 计算文件接收目录
 *
 * 优先根据 /proc/self/exe 找到程序所在目录，再回退到当前工作目录。
 * 这样无论从项目根目录还是 bin/ 目录启动，接收文件都有稳定落点。
 */
static void init_download_dir(AppState *app) {
    char exe[1024];
    ssize_t n = readlink("/proc/self/exe", exe, sizeof(exe) - 1);
    if (n > 0) {
        char *slash;
        exe[n] = '\0';
        slash = strrchr(exe, '/');
        if (slash) {
            *slash = '\0';
            slash = strrchr(exe, '/');
            if (slash) {
                char *dir;
                *slash = '\0';
                dir = g_build_filename(exe, DOWNLOAD_DIR, NULL);
                g_strlcpy(app->download_dir, dir, sizeof(app->download_dir));
                g_free(dir);
                return;
            }
        }
    }
    if (getcwd(exe, sizeof(exe))) {
        char *dir = g_build_filename(exe, DOWNLOAD_DIR, NULL);
        g_strlcpy(app->download_dir, dir, sizeof(app->download_dir));
        g_free(dir);
    } else {
        g_strlcpy(app->download_dir, DOWNLOAD_DIR, sizeof(app->download_dir));
    }
}

/*
 * make_download_path — 生成不会覆盖旧文件的保存路径
 *
 * 如果 downloads/a.txt 已存在，依次尝试 1_a.txt、2_a.txt ...
 * 这样演示多次传输同名文件时不会丢失之前的文件。
 */
static void make_download_path(AppState *app, const char *safe_name, char *path, size_t path_cap) {
    char *candidate = g_build_filename(app->download_dir, safe_name ? safe_name : "file.bin", NULL);
    g_strlcpy(path, candidate, path_cap);
    g_free(candidate);
    for (int i = 1; access(path, F_OK) == 0 && i < 1000; i++) {
        char *numbered = g_strdup_printf("%d_%s", i, safe_name ? safe_name : "file.bin");
        candidate = g_build_filename(app->download_dir, numbered, NULL);
        g_strlcpy(path, candidate, path_cap);
        g_free(numbered);
        g_free(candidate);
    }
}

static void clear_list(AppState *app) {
    GtkWidget *child;
    while ((child = gtk_widget_get_first_child(app->list_box)) != NULL) {
        gtk_list_box_remove(GTK_LIST_BOX(app->list_box), child);
    }
}

static void clear_chat(AppState *app) {
    GtkWidget *child;
    while ((child = gtk_widget_get_first_child(app->chat_box)) != NULL) {
        gtk_box_remove(GTK_BOX(app->chat_box), child);
    }
}

static gboolean ui_reset_conversation(gpointer data) {
    AppState *app = (AppState *)data;
    app->current_mode = 0;
    app->current_target[0] = '\0';
    gtk_label_set_text(GTK_LABEL(app->chat_title), "请在左侧选择好友或群开始聊天");
    gtk_label_set_text(GTK_LABEL(app->chat_subtitle), "");
    clear_chat(app);
    render_sidebar(app);
    return G_SOURCE_REMOVE;
}

/*
 * choose_conversation — 切换当前聊天对象
 *
 * target 是好友账号或群名，mode=0 表示私聊，mode=1 表示群聊。
 * 切换后立刻请求历史消息，服务端会按用户权限过滤后返回 HISTORY 行。
 */
static void choose_conversation(AppState *app, const char *target, int mode) {
    app->current_mode = mode;
    snprintf(app->current_target, sizeof(app->current_target), "%s", target);
    gtk_label_set_text(GTK_LABEL(app->chat_title), target);
    gtk_label_set_text(GTK_LABEL(app->chat_subtitle), mode == 1 ? "群组聊天" : "好友私聊");
    clear_chat(app);
    app->history_allow_fallback = 0;
    app->history_all_mode = 0;
    /* 点击好友/群时自动拉取该会话历史，符合 QQ 的历史聊天体验。 */
    send_cmdf(app, "HISTORY\t%s\t%s\t80", mode == 1 ? "G" : "P", target);
}

/* row_activated — 左侧列表点击入口，根据 row 上保存的 target/mode 切换会话。 */
static void row_activated(GtkListBox *box, GtkListBoxRow *row, gpointer data) {
    AppState *app = (AppState *)data;
    const char *target = g_object_get_data(G_OBJECT(row), "target");
    int mode = GPOINTER_TO_INT(g_object_get_data(G_OBJECT(row), "mode"));
    (void)box;
    if (target) {
        choose_conversation(app, target, mode);
        if (app->active_tab == TAB_FRIENDS || app->active_tab == TAB_GROUPS) {
            render_sidebar(app);
        }
    }
}

static void add_header(AppState *app, const char *text) {
    GtkWidget *row = gtk_list_box_row_new();
    GtkWidget *label = gtk_label_new(text);
    gtk_widget_add_css_class(label, "side-section");
    gtk_label_set_xalign(GTK_LABEL(label), 0);
    gtk_widget_set_margin_start(label, 8);
    gtk_widget_set_margin_end(label, 8);
    gtk_list_box_row_set_selectable(GTK_LIST_BOX_ROW(row), FALSE);
    gtk_list_box_row_set_child(GTK_LIST_BOX_ROW(row), label);
    gtk_list_box_append(GTK_LIST_BOX(app->list_box), row);
}

enum {
    MENU_ADD_FRIEND = 1,
    MENU_INVITE_GROUP,
    MENU_CREATE_GROUP,
    MENU_SEARCH,
    MENU_FILES,
    MENU_HISTORY,
    MENU_QUIT
};

static GtkWidget *side_tab_button_new(const char *label) {
    GtkWidget *button = gtk_button_new_with_label(label);
    gtk_widget_add_css_class(button, "side-tab");
    return button;
}

static GtkWidget *menu_action_button_new(const char *label, int action, AppState *app) {
    GtkWidget *button = gtk_button_new_with_label(label);
    gtk_widget_add_css_class(button, "menu-action");
    g_object_set_data(G_OBJECT(button), "action", GINT_TO_POINTER(action));
    g_signal_connect(button, "clicked", G_CALLBACK(menu_action_clicked), app);
    return button;
}

/* add_info/add_contact/add_recent/add_file_row 都只负责创建左侧列表中的不同类型行。 */
static void add_info(AppState *app, const char *text) {
    GtkWidget *row = gtk_list_box_row_new();
    GtkWidget *label = gtk_label_new(text);
    gtk_widget_add_css_class(label, "side-muted");
    gtk_label_set_wrap(GTK_LABEL(label), TRUE);
    gtk_label_set_max_width_chars(GTK_LABEL(label), 30);
    gtk_label_set_xalign(GTK_LABEL(label), 0);
    gtk_widget_set_margin_start(label, 16);
    gtk_widget_set_margin_end(label, 16);
    gtk_widget_set_margin_top(label, 8);
    gtk_list_box_row_set_selectable(GTK_LIST_BOX_ROW(row), FALSE);
    gtk_list_box_row_set_child(GTK_LIST_BOX_ROW(row), label);
    gtk_list_box_append(GTK_LIST_BOX(app->list_box), row);
}

/* add_contact — 好友页/群组页中的正式列表项。mode=0 好友，mode=1 群。 */
static void add_contact(AppState *app, const char *name, const char *desc, int online, int mode) {
    GtkWidget *row = gtk_list_box_row_new();
    GtkWidget *box = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 10);
    GtkWidget *avatar = make_avatar(mode == 1 ? "#" : name, 48);
    GtkWidget *texts = gtk_box_new(GTK_ORIENTATION_VERTICAL, 2);
    GtkWidget *top = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 6);
    GtkWidget *name_label = gtk_label_new(name);
    GtkWidget *state = gtk_label_new(mode == 1 ? "群" : (online ? "在线" : "离线"));
    GtkWidget *desc_label = gtk_label_new(desc);

    gtk_widget_add_css_class(row, "contact-row");
    gtk_label_set_xalign(GTK_LABEL(name_label), 0);
    gtk_label_set_xalign(GTK_LABEL(desc_label), 0);
    gtk_label_set_ellipsize(GTK_LABEL(name_label), PANGO_ELLIPSIZE_END);
    gtk_label_set_ellipsize(GTK_LABEL(desc_label), PANGO_ELLIPSIZE_END);
    gtk_widget_add_css_class(state, online || mode == 1 ? "state-online" : "state-muted");
    gtk_widget_add_css_class(desc_label, "side-muted");
    gtk_widget_set_hexpand(name_label, TRUE);
    gtk_box_append(GTK_BOX(top), name_label);
    gtk_box_append(GTK_BOX(top), state);
    gtk_box_append(GTK_BOX(texts), top);
    gtk_box_append(GTK_BOX(texts), desc_label);
    gtk_box_append(GTK_BOX(box), avatar);
    gtk_box_append(GTK_BOX(box), texts);
    gtk_list_box_row_set_child(GTK_LIST_BOX_ROW(row), box);
    g_object_set_data_full(G_OBJECT(row), "target", g_strdup(name), g_free);
    g_object_set_data(G_OBJECT(row), "mode", GINT_TO_POINTER(mode));
    gtk_list_box_append(GTK_LIST_BOX(app->list_box), row);
}

/* add_recent — 消息页中的“最近会话”入口，点击后同样进入私聊或群聊。 */
static void add_recent(AppState *app, const char *name, const char *desc, const char *stamp,
                       int online, int mode) {
    GtkWidget *row = gtk_list_box_row_new();
    GtkWidget *box = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 12);
    GtkWidget *avatar = make_avatar(mode == 1 ? "#" : name, 50);
    GtkWidget *texts = gtk_box_new(GTK_ORIENTATION_VERTICAL, 3);
    GtkWidget *top = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);
    GtkWidget *name_label = gtk_label_new(name);
    GtkWidget *stamp_label = gtk_label_new(stamp);
    GtkWidget *desc_label = gtk_label_new(desc);

    (void)online;
    gtk_widget_add_css_class(row, "recent-row");
    gtk_label_set_xalign(GTK_LABEL(name_label), 0);
    gtk_label_set_xalign(GTK_LABEL(desc_label), 0);
    gtk_label_set_xalign(GTK_LABEL(stamp_label), 1);
    gtk_label_set_ellipsize(GTK_LABEL(name_label), PANGO_ELLIPSIZE_END);
    gtk_label_set_ellipsize(GTK_LABEL(desc_label), PANGO_ELLIPSIZE_END);
    gtk_widget_add_css_class(stamp_label, "recent-time");
    gtk_widget_add_css_class(desc_label, "recent-preview");
    gtk_widget_set_hexpand(name_label, TRUE);
    gtk_box_append(GTK_BOX(top), name_label);
    gtk_box_append(GTK_BOX(top), stamp_label);
    gtk_box_append(GTK_BOX(texts), top);
    gtk_box_append(GTK_BOX(texts), desc_label);
    gtk_box_append(GTK_BOX(box), avatar);
    gtk_box_append(GTK_BOX(box), texts);
    gtk_list_box_row_set_child(GTK_LIST_BOX_ROW(row), box);
    g_object_set_data_full(G_OBJECT(row), "target", g_strdup(name), g_free);
    g_object_set_data(G_OBJECT(row), "mode", GINT_TO_POINTER(mode));
    gtk_list_box_append(GTK_LIST_BOX(app->list_box), row);
}

/* add_file_row — 文件页中的收发记录，不可选中，只用于展示本地保存路径。 */
static void add_file_row(AppState *app, const FileInfo *f) {
    GtkWidget *row = gtk_list_box_row_new();
    GtkWidget *box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 3);
    GtkWidget *name = gtk_label_new(f->name);
    char desc[1200];
    GtkWidget *desc_label;

    snprintf(desc, sizeof(desc), "%s %s · %s",
             f->incoming ? "收到自" : "发送给",
             f->peer[0] ? f->peer : "?",
             f->path);
    desc_label = gtk_label_new(desc);
    gtk_widget_add_css_class(row, "contact-row");
    gtk_label_set_xalign(GTK_LABEL(name), 0);
    gtk_label_set_xalign(GTK_LABEL(desc_label), 0);
    gtk_label_set_ellipsize(GTK_LABEL(name), PANGO_ELLIPSIZE_END);
    gtk_label_set_ellipsize(GTK_LABEL(desc_label), PANGO_ELLIPSIZE_END);
    gtk_widget_add_css_class(desc_label, "side-muted");
    gtk_box_append(GTK_BOX(box), name);
    gtk_box_append(GTK_BOX(box), desc_label);
    gtk_list_box_row_set_selectable(GTK_LIST_BOX_ROW(row), FALSE);
    gtk_list_box_row_set_child(GTK_LIST_BOX_ROW(row), box);
    gtk_list_box_append(GTK_LIST_BOX(app->list_box), row);
}

/*
 * render_sidebar — 根据当前导航页重绘左侧栏
 *
 * 左侧栏是本客户端最核心的 UI 状态机：
 * - 消息页：通知 + 最近会话
 * - 好友页：添加好友 + 好友列表 + 在线状态
 * - 群组页：建群/邀请/退群 + 群列表
 * - 文件页：P2P 文件收发记录
 *
 * 服务端推送 FRIEND/GROUP/COUNTS 等数据后，客户端更新本地 GPtrArray 缓存，
 * 再调用 render_sidebar() 把缓存重新渲染成 GTK 列表。
 */
static void render_sidebar(AppState *app) {
    clear_list(app);
    /* 左侧栏现在按“好友/群组/通知”展示，文件页保留为加号菜单里的辅助页面。 */
    if (app->active_tab == TAB_MESSAGES) {
        /* 消息页聚合两类内容：待处理申请/邀请，以及已有好友和群的最近会话入口。 */
        gtk_label_set_text(GTK_LABEL(app->sidebar_title), "通知");
        gtk_widget_set_visible(app->friend_tools, TRUE);
        gtk_widget_set_visible(app->friend_entry, FALSE);
        gtk_widget_set_visible(app->friend_action_button, FALSE);
        gtk_widget_set_visible(app->group_invite_entry, FALSE);
        gtk_widget_set_visible(app->group_invite_button, FALSE);
        gtk_widget_set_visible(app->delete_friend_button, FALSE);
        gtk_widget_set_visible(app->leave_group_button, FALSE);
        gtk_widget_set_visible(app->search_tools, TRUE);
        gtk_entry_set_placeholder_text(GTK_ENTRY(app->search_entry), "检索历史消息");
        add_header(app, "通知");
        if (app->requests->len == 0) {
            add_info(app, "暂无新的好友申请或群邀请。");
        } else {
            for (guint i = 0; i < app->requests->len; i++) {
                RequestInfo *req = g_ptr_array_index(app->requests, i);
                add_request_row(app, req);
            }
        }
        add_header(app, "最近会话");
        if (app->groups->len == 0 && app->friends->len == 0) {
            add_info(app, "还没有历史会话。添加好友或创建群后，这里会一直保留会话入口。");
        }
        for (guint i = 0; i < app->groups->len; i++) {
            GroupInfo *g = g_ptr_array_index(app->groups, i);
            add_recent(app, g->name, "群聊 · 点击查看历史消息", "群", 1, 1);
        }
        for (guint i = 0; i < app->friends->len; i++) {
            FriendInfo *f = g_ptr_array_index(app->friends, i);
            add_recent(app, f->user,
                       f->online ? "好友私聊 · 在线 · 点击查看历史" : "好友私聊 · 离线 · 点击查看历史",
                       f->online ? "在线" : "离线", f->online, 0);
        }
    } else if (app->active_tab == TAB_FRIENDS) {
        /* 好友页负责好友申请、好友列表和在线状态展示。 */
        gtk_label_set_text(GTK_LABEL(app->sidebar_title), "好友");
        gtk_widget_set_visible(app->friend_tools, TRUE);
        gtk_widget_set_visible(app->friend_entry, TRUE);
        gtk_widget_set_visible(app->friend_action_button, TRUE);
        gtk_entry_set_placeholder_text(GTK_ENTRY(app->friend_entry), "输入账号发送好友申请");
        gtk_button_set_label(GTK_BUTTON(app->friend_action_button), "申请");
        gtk_widget_set_visible(app->group_invite_entry, FALSE);
        gtk_widget_set_visible(app->group_invite_button, FALSE);
        gtk_widget_set_visible(app->delete_friend_button, TRUE);
        gtk_widget_set_visible(app->leave_group_button, FALSE);
        gtk_widget_set_visible(app->search_tools, FALSE);
        add_header(app, "我的好友");
        if (app->friends->len == 0) {
            add_info(app, "还没有好友，在上方输入 QQ 号添加。");
        }
        for (guint i = 0; i < app->friends->len; i++) {
            FriendInfo *f = g_ptr_array_index(app->friends, i);
            add_contact(app, f->user, f->online ? "可以开始聊天" : "离线消息会自动保存", f->online, 0);
        }
    } else if (app->active_tab == TAB_GROUPS) {
        /* 群组页负责建群、邀请入群、退群和进入群聊。 */
        gtk_label_set_text(GTK_LABEL(app->sidebar_title), "群组");
        gtk_widget_set_visible(app->friend_tools, TRUE);
        gtk_widget_set_visible(app->friend_entry, TRUE);
        gtk_widget_set_visible(app->friend_action_button, TRUE);
        gtk_entry_set_placeholder_text(GTK_ENTRY(app->friend_entry), "输入群名创建群");
        gtk_button_set_label(GTK_BUTTON(app->friend_action_button), "建群");
        gtk_entry_set_placeholder_text(GTK_ENTRY(app->group_invite_entry), "邀请账号");
        gtk_widget_set_visible(app->group_invite_entry, TRUE);
        gtk_widget_set_visible(app->group_invite_button, TRUE);
        gtk_widget_set_visible(app->delete_friend_button, FALSE);
        gtk_widget_set_visible(app->leave_group_button, TRUE);
        gtk_widget_set_visible(app->search_tools, FALSE);
        add_header(app, "我的群聊");
        if (app->groups->len == 0) {
            add_info(app, "还没有群。输入群名创建群，或等待别人邀请后同意加入。");
        }
        for (guint i = 0; i < app->groups->len; i++) {
            GroupInfo *g = g_ptr_array_index(app->groups, i);
            add_contact(app, g->name, "私有群 · 邀请加入", 1, 1);
        }
    } else {
        gtk_label_set_text(GTK_LABEL(app->sidebar_title), "文件");
        gtk_widget_set_visible(app->friend_tools, FALSE);
        gtk_widget_set_visible(app->delete_friend_button, FALSE);
        gtk_widget_set_visible(app->leave_group_button, FALSE);
        gtk_widget_set_visible(app->search_tools, FALSE);
        add_header(app, "文件传输");
        add_info(app, "先在消息或好友里选中一个在线好友，再点击底部“文件”发送。接收文件会保存在工程目录 downloads/。");
        if (app->files->len == 0) {
            add_info(app, "还没有文件记录。收到或发送成功后会显示在这里。");
        }
        for (guint i = 0; i < app->files->len; i++) {
            FileInfo *f = g_ptr_array_index(app->files, i);
            add_file_row(app, f);
        }
    }
}

/* set_tab — 切换导航页，并同步按钮高亮和左侧栏内容。 */
static void set_tab(AppState *app, int tab) {
    app->active_tab = tab;
    for (int i = 0; i < 4; i++) {
        if (!app->nav_buttons[i]) {
            continue;
        }
        if (i == tab) {
            gtk_widget_add_css_class(app->nav_buttons[i], "nav-active");
        } else {
            gtk_widget_remove_css_class(app->nav_buttons[i], "nav-active");
        }
    }
    render_sidebar(app);
}

static gboolean ui_choose_conversation(gpointer data) {
    UiChoose *ui = (UiChoose *)data;
    set_tab(ui->app, ui->tab);
    choose_conversation(ui->app, ui->target, ui->mode);
    g_free(ui);
    return G_SOURCE_REMOVE;
}

/* select_conversation_async — 供 receiver_main 等后台线程安全地切换会话。 */
static void select_conversation_async(AppState *app, const char *target, int mode, int tab) {
    UiChoose *ui;
    if (!target || !*target) {
        return;
    }
    ui = g_new0(UiChoose, 1);
    ui->app = app;
    ui->mode = mode;
    ui->tab = tab;
    snprintf(ui->target, sizeof(ui->target), "%s", target);
    g_idle_add(ui_choose_conversation, ui);
}

static void nav_clicked(GtkButton *button, gpointer data) {
    AppState *app = (AppState *)data;
    int tab = GPOINTER_TO_INT(g_object_get_data(G_OBJECT(button), "tab"));
    set_tab(app, tab);
}

static void add_friend_clicked(GtkButton *button, gpointer data) {
    AppState *app = (AppState *)data;
    const char *name = gtk_editable_get_text(GTK_EDITABLE(app->friend_entry));
    (void)button;
    if (app->active_tab == TAB_GROUPS) {
        if (name && *name) {
            if (strcmp(name, "lobby") == 0) {
                append_notice(app, "公共群 lobby 已移除，请输入自己的群名。");
                return;
            }
            send_cmdf(app, "CREATE_GROUP\t%s", name);
            append_notice(app, "已提交建群请求：%s", name);
            gtk_editable_set_text(GTK_EDITABLE(app->friend_entry), "");
        }
    } else if (name && *name) {
        send_cmdf(app, "ADD_FRIEND\t%s", name);
        append_notice(app, "已发送好友申请：%s", name);
        gtk_editable_set_text(GTK_EDITABLE(app->friend_entry), "");
    }
}

static void invite_group_clicked(GtkButton *button, gpointer data) {
    AppState *app = (AppState *)data;
    const char *name = gtk_editable_get_text(GTK_EDITABLE(app->group_invite_entry));
    (void)button;
    if (!app->current_target[0] || app->current_mode != 1) {
        append_notice(app, "请先在群聊页选择一个群，再邀请好友。");
        return;
    }
    if (name && *name) {
        send_cmdf(app, "INVITE_GROUP\t%s\t%s", app->current_target, name);
        append_notice(app, "已邀请 %s 加入群 %s", name, app->current_target);
        gtk_editable_set_text(GTK_EDITABLE(app->group_invite_entry), "");
    }
}

static void delete_friend_clicked(GtkButton *button, gpointer data) {
    AppState *app = (AppState *)data;
    (void)button;
    if (!app->current_target[0] || app->current_mode != 0) {
        append_notice(app, "请先在好友列表中选择一个好友。");
        return;
    }
    send_cmdf(app, "DELETE_FRIEND\t%s", app->current_target);
}

static void leave_group_clicked(GtkButton *button, gpointer data) {
    AppState *app = (AppState *)data;
    (void)button;
    if (!app->current_target[0] || app->current_mode != 1) {
        append_notice(app, "请先在群聊列表中选择一个群。");
        return;
    }
    send_cmdf(app, "LEAVE_GROUP\t%s", app->current_target);
}

static void quit_clicked(GtkButton *button, gpointer data) {
    AppState *app = (AppState *)data;
    (void)button;
    if (app->sockfd >= 0) {
        send_cmd(app, "QUIT");
    }
    g_application_quit(G_APPLICATION(app->gtk_app));
}

/*
 * send_clicked — 发送聊天消息
 *
 * 用户输入的正文会先经过 qq_escape() 转义，避免正文中的 TAB/换行破坏协议格式。
 * 私聊使用 MSGP，群聊使用 MSGG；服务端负责持久化、转发和离线保存。
 */
static void send_clicked(GtkButton *button, gpointer data) {
    AppState *app = (AppState *)data;
    const char *text = gtk_editable_get_text(GTK_EDITABLE(app->message_entry));
    char *enc;
    (void)button;
    if (!text || !*text) {
        return;
    }
    if (!app->current_target[0]) {
        append_notice(app, "请先从左侧选择一个好友或群聊。");
        return;
    }
    enc = qq_escape(text);
    /* current_mode 由左侧点击好友/群时设置：0 发 MSGP，1 发 MSGG。 */
    if (app->current_mode == 1) {
        send_cmdf(app, "MSGG\t%s\t%s", app->current_target, enc ? enc : text);
    } else {
        send_cmdf(app, "MSGP\t%s\t%s", app->current_target, enc ? enc : text);
    }
    free(enc);
    gtk_editable_set_text(GTK_EDITABLE(app->message_entry), "");
}

static void message_activate(GtkEntry *entry, gpointer data) {
    (void)entry;
    send_clicked(NULL, data);
}

static void file_dialog_response(GtkNativeDialog *dialog, int response, gpointer data) {
    AppState *app = (AppState *)data;
    if (response == GTK_RESPONSE_ACCEPT) {
        GFile *file = gtk_file_chooser_get_file(GTK_FILE_CHOOSER(dialog));
        if (file) {
            char *path = g_file_get_path(file);
            if (path) {
                FileSendJob *job = g_new0(FileSendJob, 1);
                pthread_t tid;
                job->app = app;
                snprintf(job->to, sizeof(job->to), "%s", app->current_target);
                snprintf(job->path, sizeof(job->path), "%s", path);
                /* 发送文件可能耗时，放到独立线程，避免 GTK 主线程卡住。 */
                pthread_create(&tid, NULL, file_send_main, job);
                pthread_detach(tid);
                g_free(path);
            }
            g_object_unref(file);
        }
    }
    gtk_native_dialog_destroy(dialog);
}

static void send_file_clicked(GtkButton *button, gpointer data) {
    AppState *app = (AppState *)data;
    GtkFileChooserNative *dialog;
    (void)button;
    if (!app->current_target[0] || app->current_mode == 1) {
        append_notice(app, "请选择一个在线好友后再发送文件。");
        return;
    }
    dialog = gtk_file_chooser_native_new("选择要发送的文件",
                                         GTK_WINDOW(app->main_window),
                                         GTK_FILE_CHOOSER_ACTION_OPEN,
                                         "发送",
                                         "取消");
    g_signal_connect(dialog, "response", G_CALLBACK(file_dialog_response), app);
    gtk_native_dialog_show(GTK_NATIVE_DIALOG(dialog));
}

static void history_clicked(GtkButton *button, gpointer data) {
    AppState *app = (AppState *)data;
    (void)button;
    clear_chat(app);
    app->current_mode = 0;
    app->current_target[0] = '\0';
    app->history_allow_fallback = 0;
    app->history_all_mode = 1;
    gtk_label_set_text(GTK_LABEL(app->chat_title), "全部历史消息");
    gtk_label_set_text(GTK_LABEL(app->chat_subtitle), "当前账号相关的历史记录");
    send_cmd(app, "HISTORY\tALL\t*\t1000");
}

static void search_clicked(GtkButton *button, gpointer data) {
    AppState *app = (AppState *)data;
    const char *kw = gtk_editable_get_text(GTK_EDITABLE(app->search_entry));
    char *enc;
    (void)button;
    if (kw && *kw) {
        enc = qq_escape(kw);
        clear_chat(app);
        gtk_label_set_text(GTK_LABEL(app->chat_title), "消息检索");
        gtk_label_set_text(GTK_LABEL(app->chat_subtitle), kw);
        send_cmdf(app, "SEARCH\t%s", enc ? enc : kw);
        free(enc);
    }
}

static void menu_action_clicked(GtkButton *button, gpointer data) {
    AppState *app = (AppState *)data;
    GtkWidget *popover = gtk_widget_get_ancestor(GTK_WIDGET(button), GTK_TYPE_POPOVER);
    int action = GPOINTER_TO_INT(g_object_get_data(G_OBJECT(button), "action"));
    if (popover) {
        gtk_popover_popdown(GTK_POPOVER(popover));
    }
    switch (action) {
    case MENU_ADD_FRIEND:
        set_tab(app, TAB_FRIENDS);
        gtk_widget_grab_focus(app->friend_entry);
        break;
    case MENU_INVITE_GROUP:
        set_tab(app, TAB_GROUPS);
        gtk_widget_grab_focus(app->group_invite_entry);
        append_notice(app, "先选择一个群，再输入账号邀请好友入群。");
        break;
    case MENU_CREATE_GROUP:
        set_tab(app, TAB_GROUPS);
        gtk_widget_grab_focus(app->friend_entry);
        break;
    case MENU_SEARCH:
        set_tab(app, TAB_MESSAGES);
        gtk_widget_grab_focus(app->search_entry);
        break;
    case MENU_FILES:
        set_tab(app, TAB_FILES);
        break;
    case MENU_HISTORY:
        history_clicked(NULL, app);
        break;
    case MENU_QUIT:
        quit_clicked(NULL, app);
        break;
    default:
        break;
    }
}

static gboolean ui_clear_friends(gpointer data) {
    AppState *app = (AppState *)data;
    g_ptr_array_set_size(app->friends, 0);
    render_sidebar(app);
    return G_SOURCE_REMOVE;
}

static gboolean ui_add_friend(gpointer data) {
    UiFriend *ui = (UiFriend *)data;
    g_ptr_array_add(ui->app->friends, ui->friend_info);
    render_sidebar(ui->app);
    g_free(ui);
    return G_SOURCE_REMOVE;
}

static gboolean ui_peer_info(gpointer data) {
    PeerInfo *p = (PeerInfo *)data;
    pthread_mutex_lock(&p->app->peer_mutex);
    snprintf(p->app->peer_user, sizeof(p->app->peer_user), "%s", p->user);
    snprintf(p->app->peer_ip, sizeof(p->app->peer_ip), "%s", p->ip);
    p->app->peer_port = p->port;
    p->app->peer_online = p->online;
    p->app->peer_ready = 1;
    pthread_cond_broadcast(&p->app->peer_cond);
    pthread_mutex_unlock(&p->app->peer_mutex);
    g_free(p);
    return G_SOURCE_REMOVE;
}

/*
 * setup_file_listener — 初始化 P2P 文件接收 socket
 *
 * 绑定端口 0 表示让系统自动分配一个可用端口。
 * 分配成功后通过 getsockname() 取回真实端口号，登录时携带给服务端，
 * 其他客户端发送文件前会通过 PEER_INFO 查询到这个端口。
 */
static int setup_file_listener(AppState *app) {
    struct sockaddr_in addr;
    socklen_t len;
    int opt = 1;

    /* P2P 接收端监听随机端口，登录时把端口号告诉服务器。 */
    app->file_listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (app->file_listen_fd < 0) {
        return -1;
    }
    setsockopt(app->file_listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = 0;
    if (bind(app->file_listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0 ||
        listen(app->file_listen_fd, 16) < 0) {
        close(app->file_listen_fd);
        app->file_listen_fd = -1;
        return -1;
    }
    len = sizeof(addr);
    if (getsockname(app->file_listen_fd, (struct sockaddr *)&addr, &len) < 0) {
        close(app->file_listen_fd);
        app->file_listen_fd = -1;
        return -1;
    }
    app->file_port = ntohs(addr.sin_port);
    mkdir(app->download_dir, 0755);
    return 0;
}

/*
 * file_listener_main — P2P 文件接收线程
 *
 * 接收端协议格式：
 *   QQFILE\tfrom\tfilename_enc\tsize\n
 *   后续紧跟 size 字节二进制文件数据
 *
 * 注意文件数据没有经过服务端，服务端只负责告诉发送方接收方 IP/端口。
 */
static void *file_listener_main(void *arg) {
    AppState *app = (AppState *)arg;
    /* 常驻文件接收线程：接收 QQFILE 头后把文件保存到 downloads/。 */
    while (!app->shutting_down) {
        int fd = accept(app->file_listen_fd, NULL, NULL);
        char header[QQ_MAX_LINE];
        char *cmd;
        char *from;
        char *name_enc;
        char *size_s;
        char *name;
        char *safe_name;
        char path[1200];
        FILE *out;
        long remain;

        if (fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        if (qq_recv_line(fd, header, sizeof(header)) <= 0) {
            close(fd);
            continue;
        }
        cmd = strtok(header, "\t");
        from = strtok(NULL, "\t");
        name_enc = strtok(NULL, "\t");
        size_s = strtok(NULL, "\t");
        if (!cmd || strcmp(cmd, "QQFILE") != 0 || !from || !name_enc || !size_s) {
            close(fd);
            continue;
        }
        name = qq_unescape(name_enc);
        safe_name = qq_basename_dup(name ? name : "file.bin");
        make_download_path(app, safe_name ? safe_name : "file.bin", path, sizeof(path));
        out = fopen(path, "wb");
        remain = atol(size_s);
        if (out) {
            char buf[4096];
            while (remain > 0) {
                ssize_t n = recv(fd, buf, remain > (long)sizeof(buf) ? sizeof(buf) : (size_t)remain, 0);
                if (n <= 0) {
                    break;
                }
                fwrite(buf, 1, (size_t)n, out);
                remain -= n;
            }
            fclose(out);
            append_notice(app, "文件: 已从 %s 接收 %s", from, path);
            add_file_record(app, safe_name ? safe_name : "file.bin", path, from, 1);
        } else {
            append_notice(app, "文件: 无法保存到 %s: %s", path, strerror(errno));
        }
        free(name);
        free(safe_name);
        close(fd);
    }
    return NULL;
}

/*
 * wait_peer — 等待服务端返回目标用户 P2P 地址
 *
 * 本函数运行在文件发送线程：
 *   1. 发送 PEER_INFO user 到服务端
 *   2. 用条件变量等待 receiver_main() 收到 PEER 响应
 *   3. 超过 5 秒仍未收到则认为查询失败
 *
 * 这里用条件变量而不是忙等，避免后台线程空转消耗 CPU。
 */
static int wait_peer(AppState *app, const char *user, char *ip, size_t ip_cap, int *port) {
    struct timespec ts;
    int ok = 0;

    pthread_mutex_lock(&app->peer_mutex);
    app->peer_ready = 0;
    /* 先通过服务器查询对方 P2P 地址，再由客户端直接连接对方。 */
    send_cmdf(app, "PEER_INFO\t%s", user);
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 5;
    while (!app->peer_ready) {
        if (pthread_cond_timedwait(&app->peer_cond, &app->peer_mutex, &ts) == ETIMEDOUT) {
            break;
        }
    }
    if (app->peer_ready && app->peer_online && strcmp(app->peer_user, user) == 0 && app->peer_port > 0) {
        snprintf(ip, ip_cap, "%s", app->peer_ip);
        *port = app->peer_port;
        ok = 1;
    }
    pthread_mutex_unlock(&app->peer_mutex);
    return ok;
}

/*
 * file_send_main — P2P 文件发送线程
 *
 * 流程：
 *   1. 通过 wait_peer() 查询接收方在线状态、IP 和 P2P 端口
 *   2. 直接 connect 接收方客户端的文件监听端口
 *   3. 先给服务端发 FILE_NOTICE，作为聊天窗口里的文件通知
 *   4. 再通过 P2P socket 发送 QQFILE 头和文件二进制内容
 */
static void *file_send_main(void *arg) {
    FileSendJob *job = (FileSendJob *)arg;
    char ip[64];
    int port = 0;
    int fd;
    FILE *in;
    long size;
    char *base;
    char *enc;
    struct sockaddr_in addr;

    /* 每次发送文件启动独立线程，避免大文件阻塞 GTK 主界面。 */
    if (!wait_peer(job->app, job->to, ip, sizeof(ip), &port)) {
        append_notice(job->app, "文件: %s 不在线或未开启 P2P 端口", job->to);
        g_free(job);
        return NULL;
    }
    in = fopen(job->path, "rb");
    if (!in) {
        append_notice(job->app, "文件: 无法打开 %s", job->path);
        g_free(job);
        return NULL;
    }
    fseek(in, 0, SEEK_END);
    size = ftell(in);
    fseek(in, 0, SEEK_SET);
    base = qq_basename_dup(job->path);
    enc = qq_escape(base);

    fd = socket(AF_INET, SOCK_STREAM, 0);
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    inet_pton(AF_INET, ip, &addr.sin_addr);
    if (fd < 0 || connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        append_notice(job->app, "文件: P2P 连接 %s:%d 失败", ip, port);
        if (fd >= 0) {
            close(fd);
        }
        fclose(in);
        free(base);
        free(enc);
        g_free(job);
        return NULL;
    }
    send_cmdf(job->app, "FILE_NOTICE\t%s\t%s\t%ld", job->to, enc ? enc : base, size);
    dprintf(fd, "QQFILE\t%s\t%s\t%ld\n", job->app->user, enc ? enc : base, size);
    while (!feof(in)) {
        char buf[4096];
        size_t n = fread(buf, 1, sizeof(buf), in);
        if (n > 0 && qq_send_all(fd, buf, n) < 0) {
            break;
        }
    }
    append_notice(job->app, "文件: 已向 %s 发送 %s", job->to, base);
    add_file_record(job->app, base, job->path, job->to, 0);
    close(fd);
    fclose(in);
    free(base);
    free(enc);
    g_free(job);
    return NULL;
}

/*
 * handle_message — 处理实时聊天消息
 *
 * 服务端下发 MSG kind from scope ts text_enc。
 * 客户端负责反转义正文、格式化时间、判断是否自己发送，然后追加聊天气泡。
 */
static void handle_message(AppState *app, char **parts) {
    char *kind = parts[1];
    char *from = parts[2];
    char *scope = parts[3];
    char *ts = parts[4];
    char *enc = parts[5];
    char *text = qq_unescape(enc);
    char *clock = clock_label(ts ? atol(ts) : time(NULL));
    char title[256];
    int outgoing = from && strcmp(from, app->user) == 0;
    if (client_kind_is_group(kind)) {
        snprintf(title, sizeof(title), "%s  群 %s | %s", clock, scope, from);
    } else {
        snprintf(title, sizeof(title), "%s  %s", clock, outgoing ? "我" : from);
    }
    append_chat(app, outgoing, from, title, text ? text : enc);
    free(text);
    g_free(clock);
}

/*
 * handle_history — 处理历史消息和搜索命中
 *
 * HISTORY 和 SEARCH 的字段结构与 MSG 类似，因此复用同一套反转义和时间格式化逻辑。
 * prefix="历史" 时按聊天气泡显示；prefix="命中" 时作为检索结果提示显示。
 */
static void handle_history(AppState *app, const char *prefix, char **parts) {
    char *kind = parts[1];
    char *from = parts[2];
    char *scope = parts[3];
    char *ts = parts[4];
    char *enc = parts[5];
    char *text = qq_unescape(enc);
    char *clock = clock_label(ts ? atol(ts) : time(NULL));
    if (strcmp(prefix, "历史") == 0) {
        char title[256];
        int outgoing = from && strcmp(from, app->user) == 0;
        app->history_rows++;
        if (client_kind_is_group(kind)) {
            snprintf(title, sizeof(title), "%s  群 %s | %s", clock, scope, from);
        } else {
            snprintf(title, sizeof(title), "%s  %s", clock, outgoing ? "我" : from);
        }
        append_chat(app, outgoing, from, title, text ? text : enc);
    } else {
        append_notice(app, "%s [%s] %s %s -> %s: %s",
                      prefix, clock, client_kind_is_group(kind) ? "群聊" : "私聊",
                      from, scope, text ? text : enc);
    }
    free(text);
    g_free(clock);
}

/*
 * receiver_main — 服务端协议接收与分发线程
 *
 * 该线程只负责读取和解析 app->sockfd 上的协议行：
 *   FRIEND/GROUP/COUNTS 更新侧栏缓存
 *   MSG/OFFLINE/HISTORY/SEARCH 更新聊天区
 *   FRIEND_REQUEST/GROUP_INVITE 更新通知列表
 *   PEER 唤醒等待中的文件发送线程
 *
 * 所有 GTK 控件更新都通过 g_idle_add() 回到主线程执行。
 */
static void *receiver_main(void *arg) {
    AppState *app = (AppState *)arg;
    char line[QQ_MAX_LINE];
    /* 后台接收线程只读服务器消息，真正改 GTK 控件时通过 g_idle_add 回到主线程。 */
    while (!app->shutting_down && qq_recv_line(app->sockfd, line, sizeof(line)) > 0) {
        char *parts[8] = {0};
        char *p;
        int i = 0;
        qq_trim_newline(line);
        p = strtok(line, "\t");
        while (p && i < 8) {
            parts[i++] = p;
            p = strtok(NULL, "\t");
        }
        if (!parts[0]) {
            continue;
        }
        /*
         * 接收线程只负责解析服务器协议。凡是要修改 GTK 控件的操作，
         * 都封装成小对象后用 g_idle_add 投递回主线程执行。
         */
        if (strcmp(parts[0], "OK") == 0) {
            if (parts[1] && parts[2] &&
                (strcmp(parts[1], "CREATE_GROUP") == 0 || strcmp(parts[1], "ACCEPT_GROUP") == 0)) {
                select_conversation_async(app, parts[2], 1, TAB_GROUPS);
            } else if (parts[1] && parts[2] &&
                       (strcmp(parts[1], "DELETE_FRIEND") == 0 || strcmp(parts[1], "LEAVE_GROUP") == 0) &&
                       strcmp(parts[2], app->current_target) == 0) {
                g_idle_add(ui_reset_conversation, app);
            }
            append_notice(app, "%s %s", parts[1] ? parts[1] : "OK", parts[2] ? parts[2] : "");
        } else if (strcmp(parts[0], "ERR") == 0) {
            append_notice(app, "错误: %s", parts[1] ? parts[1] : "unknown");
        } else if (strcmp(parts[0], "FRIENDS_BEGIN") == 0) {
            g_idle_add(ui_clear_friends, app);
        } else if (strcmp(parts[0], "FRIEND") == 0 && parts[1] && parts[2]) {
            UiFriend *ui = g_new0(UiFriend, 1);
            FriendInfo *f = g_new0(FriendInfo, 1);
            snprintf(f->user, sizeof(f->user), "%s", parts[1]);
            f->online = atoi(parts[2]);
            ui->app = app;
            ui->friend_info = f;
            g_idle_add(ui_add_friend, ui);
        } else if (strcmp(parts[0], "GROUPS_BEGIN") == 0) {
            g_idle_add(ui_clear_groups, app);
        } else if (strcmp(parts[0], "GROUP") == 0 && parts[1]) {
            UiGroup *ui = g_new0(UiGroup, 1);
            GroupInfo *g = g_new0(GroupInfo, 1);
            snprintf(g->name, sizeof(g->name), "%s", parts[1]);
            ui->app = app;
            ui->group_info = g;
            g_idle_add(ui_add_group, ui);
        } else if (strcmp(parts[0], "COUNTS") == 0 && parts[1] && parts[2]) {
            UiCounts *ui = g_new0(UiCounts, 1);
            ui->app = app;
            ui->friends = atoi(parts[1]);
            ui->online_friends = atoi(parts[2]);
            g_idle_add(ui_update_counts, ui);
        } else if (strcmp(parts[0], "FRIEND_REQUEST") == 0 && parts[1]) {
            UiRequest *r = g_new0(UiRequest, 1);
            r->app = app;
            snprintf(r->type, sizeof(r->type), "friend");
            snprintf(r->from, sizeof(r->from), "%s", parts[1]);
            g_idle_add(ui_append_request, r);
        } else if (strcmp(parts[0], "GROUP_INVITE") == 0 && parts[1] && parts[2]) {
            UiRequest *r = g_new0(UiRequest, 1);
            r->app = app;
            snprintf(r->type, sizeof(r->type), "group");
            snprintf(r->group, sizeof(r->group), "%s", parts[1]);
            snprintf(r->from, sizeof(r->from), "%s", parts[2]);
            g_idle_add(ui_append_request, r);
        } else if (strcmp(parts[0], "MSG") == 0 && parts[5]) {
            handle_message(app, parts);
        } else if (strcmp(parts[0], "OFFLINE") == 0 && parts[5]) {
            char *fake[6] = {"MSG", parts[1], parts[2], parts[3], parts[4], parts[5]};
            append_notice(app, "离线消息");
            handle_message(app, fake);
        } else if (strcmp(parts[0], "HISTORY_BEGIN") == 0) {
            app->history_rows = 0;
        } else if (strcmp(parts[0], "HISTORY") == 0 && parts[5]) {
            handle_history(app, "历史", parts);
        } else if (strcmp(parts[0], "HISTORY_END") == 0) {
            if (app->history_rows == 0 && app->history_allow_fallback && !app->history_all_mode) {
                app->history_allow_fallback = 0;
                app->history_all_mode = 1;
                append_notice(app, "当前会话没有精确历史，显示你的最近历史消息");
                send_cmd(app, "HISTORY\tALL\t*\t100");
            } else if (app->history_rows == 0) {
                append_notice(app, "暂无历史消息");
            }
        } else if (strcmp(parts[0], "SEARCH_BEGIN") == 0) {
            append_notice(app, "检索结果");
        } else if (strcmp(parts[0], "SEARCH") == 0 && parts[5]) {
            handle_history(app, "命中", parts);
        } else if (strcmp(parts[0], "NOTICE") == 0 && parts[2]) {
            char *text = qq_unescape(parts[2]);
            append_notice(app, "%s", text ? text : parts[2]);
            free(text);
        } else if (strcmp(parts[0], "PEER") == 0 && parts[4]) {
            PeerInfo *pi = g_new0(PeerInfo, 1);
            pi->app = app;
            snprintf(pi->user, sizeof(pi->user), "%s", parts[1]);
            snprintf(pi->ip, sizeof(pi->ip), "%s", parts[2]);
            pi->port = atoi(parts[3]);
            pi->online = atoi(parts[4]);
            g_idle_add(ui_peer_info, pi);
        } else if (strcmp(parts[0], "BYE") == 0) {
            break;
        }
    }
    if (!app->shutting_down) {
        append_notice(app, "与服务器连接已断开");
    }
    return NULL;
}

/*
 * auth — 登录/注册握手
 *
 * LOGIN/REGISTER 是控制长连接上的第一条命令。
 * 服务端验证成功后返回 OK，随后客户端才启动 receiver_main() 接收线程。
 */
static int auth(AppState *app, int reg, char *err, size_t err_cap) {
    char line[QQ_MAX_LINE];
    char *display_enc = qq_escape(app->user);
    char *pass_enc = qq_escape(app->password);
    const char *cmd = reg ? "REGISTER" : "LOGIN";

    app->sockfd = connect_to_server(app->host, app->port);
    if (app->sockfd < 0) {
        snprintf(err, err_cap, "无法连接服务器 %s:%d，请先启动 ./bin/qq_server 9090 8", app->host, app->port);
        free(display_enc);
        free(pass_enc);
        return 0;
    }
    send_cmdf(app, "%s\t%s\t%s\t%d\t%s", cmd, app->user, display_enc ? display_enc : app->user,
              app->file_port,
              pass_enc ? pass_enc : app->password);
    free(display_enc);
    free(pass_enc);
    if (qq_recv_line(app->sockfd, line, sizeof(line)) <= 0) {
        snprintf(err, err_cap, "服务器没有返回登录结果");
        close(app->sockfd);
        app->sockfd = -1;
        return 0;
    }
    qq_trim_newline(line);
    if (strncmp(line, "OK\t", 3) == 0) {
        return 1;
    }
    g_strlcpy(err, strncmp(line, "ERR\t", 4) == 0 ? line + 4 : line, err_cap);
    close(app->sockfd);
    app->sockfd = -1;
    return 0;
}

/*
 * apply_css — 注册应用级 CSS
 *
 * GTK4 通过 CSS Provider 管理控件样式。本项目没有单独拆出 .css 文件，
 * 是为了课程演示时只编译 C 源码即可运行；代价是样式字符串较长。
 */
static void apply_css(void) {
    GtkCssProvider *provider = gtk_css_provider_new();
    const char *css =
        "* { font-family: Sans; }"
        "window { background: #edf3f8; }"
        "button { box-shadow: none; }"
        ".login { background: #2b1631; color: #eee8f1; }"
        ".login-top button { background: transparent; color: #d7cdda; border: 0; box-shadow: none; font-size: 24px; }"
        ".login-card { background: #2b1631; }"
        ".login-entry { background: rgba(255,255,255,0.12); border-radius: 10px; padding: 0 16px; min-height: 60px; }"
        ".login-entry entry { background: transparent; color: #f4f1f6; border: 0; box-shadow: none; font-size: 24px; font-weight: 700; }"
        ".login-entry image { color: #ded5e2; }"
        ".login-check { color: #c9bfcd; font-size: 15px; }"
        ".login-check check { background: #0b83dd; border-radius: 12px; border: 0; }"
        ".login-main-button { background: #0878d8; color: white; border: 0; border-radius: 10px; font-size: 24px; font-weight: 700; min-height: 58px; }"
        ".login-main-button:hover { background: #0b8df7; }"
        ".login-link { background: transparent; color: #178dff; border: 0; box-shadow: none; font-size: 18px; }"
        ".login-error { color: #ffb4c1; font-size: 13px; }"
        ".shell { background: #f5f9fd; }"
        ".side { background: #ffffff; border-right: 2px solid #d8d8d8; }"
        ".profile { background: #ffffff; color: #1f2933; padding: 20px 18px; border-bottom: 1px solid #e5edf3; }"
        ".profile-name { color: #26323f; font-size: 22px; font-weight: 800; }"
        ".profile-sub { color: #8c9aa8; font-size: 13px; }"
        ".round-add { min-width: 42px; min-height: 42px; padding: 0; }"
        "menubutton.round-add button { background: #1dade2; color: white; border: 0; border-radius: 21px; min-width: 42px; min-height: 42px; padding: 0; box-shadow: 0 2px 7px rgba(29,173,226,0.24); }"
        "menubutton.round-add button:hover { background: #129bd5; }"
        ".round-add-label { color: white; font-size: 24px; font-weight: 500; margin-bottom: 2px; }"
        ".plus-menu { background: white; padding: 0; border: 1px solid #d8d8d8; box-shadow: 0 6px 20px rgba(0,0,0,0.12); }"
        ".menu-action { background: white; color: #3d4650; border: 0; border-radius: 0; min-width: 132px; min-height: 42px; padding: 0 20px; font-size: 15px; }"
        ".menu-action:hover { background: #edf7fd; color: #15a3ed; }"
        ".side-tabs { background: #e7e3e0; border-bottom: 1px solid #d0d0d0; }"
        ".side-tab { background: transparent; color: #7d8793; border: 0; border-radius: 0; min-height: 58px; padding: 0 28px; font-size: 17px; font-weight: 700; }"
        ".side-tab:hover { background: rgba(255,255,255,0.45); color: #23aee5; }"
        ".side-tab.nav-active { background: #ffffff; color: #20aae3; border-bottom: 4px solid #20aae3; }"
        ".side-title { color: #26384b; font-size: 15px; font-weight: 800; margin: 14px 18px 8px; }"
        ".side-section { color: #8b98a5; font-size: 12px; font-weight: 800; padding: 8px 14px 7px; }"
        ".side-muted { color: #9aa7b4; font-size: 13px; }"
        ".contact-row { padding: 14px 18px; border-radius: 0; margin: 0; border-bottom: 1px solid #edf1f5; }"
        ".contact-row:hover { background: #f0f8fe; }"
        ".recent-row { padding: 13px 14px; border-radius: 0; margin: 0; border-bottom: 1px solid #edf1f5; }"
        ".recent-row:hover { background: #f0f8fe; }"
        ".recent-time { color: #9aa2ad; font-size: 12px; }"
        ".recent-preview { color: #8f98a4; font-size: 13px; }"
        ".request-row { background: #f4f9fd; border-bottom: 1px solid #e1edf6; }"
        ".request-title { color: #33485c; font-size: 13px; font-weight: 700; }"
        ".state-online { color: #12a97e; font-size: 12px; }"
        ".state-muted { color: #8a96a3; font-size: 12px; }"
        ".chat-main { background: #f5f9fd; }"
        ".chat-head { background: #27afe3; border-bottom: 1px solid #1a9fce; padding: 18px 28px; min-height: 62px; }"
        ".chat-title { font-size: 18px; font-weight: 800; color: white; }"
        ".chat-subtitle { color: rgba(255,255,255,0.82); font-size: 12px; }"
        ".chat-area { background: #f5f9fd; padding: 14px 0; }"
        ".composer { background: #eef7fd; border-top: 1px solid #d7e6ef; padding: 12px 16px; }"
        ".bubble-in { background: #ffffff; color: #1f2d3d; border-radius: 12px; padding: 10px 14px; }"
        ".bubble-out { background: #b8ebff; color: #163247; border-radius: 12px; padding: 10px 14px; }"
        ".sys-badge { background: #dbe7f1; color: #607386; border-radius: 13px; padding: 6px 13px; font-size: 12px; }"
        ".primary { background: #1dade2; color: white; border: 0; border-radius: 8px; font-weight: 700; min-height: 40px; padding: 0 22px; }"
        ".primary:hover { background: #0f89d2; }"
        ".ghost { background: #f5f9fd; color: #34495e; border: 1px solid #d4e0eb; border-radius: 8px; min-height: 36px; padding: 0 16px; }"
        ".ghost:hover { background: #eaf5ff; }"
        ".icon-tool { background: #ffffff; color: #329bc4; border: 1px solid #d5e3ec; border-radius: 8px; min-width: 52px; min-height: 46px; }"
        "entry { border-radius: 6px; padding: 11px 16px; border: 1px solid #d3e0e9; background: white; box-shadow: none; }"
        "listbox { background: transparent; }"
        "listbox row { background: transparent; }";

    gtk_css_provider_load_from_data(provider, css, -1);
    gtk_style_context_add_provider_for_display(gdk_display_get_default(), GTK_STYLE_PROVIDER(provider),
                                               GTK_STYLE_PROVIDER_PRIORITY_APPLICATION);
    g_object_unref(provider);
}

/* icon_entry — 登录页带图标输入框，复用 GTK 内置 symbolic 图标。 */
static GtkWidget *icon_entry(const char *icon_name, GtkWidget *entry) {
    GtkWidget *box = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 10);
    GtkWidget *icon = gtk_image_new_from_icon_name(icon_name);
    gtk_widget_add_css_class(box, "login-entry");
    gtk_widget_set_hexpand(entry, TRUE);
    gtk_box_append(GTK_BOX(box), icon);
    gtk_box_append(GTK_BOX(box), entry);
    return box;
}

static void close_login(GtkButton *button, gpointer data) {
    (void)button;
    g_application_quit(G_APPLICATION(data));
}

static void build_main(AppState *app);

/*
 * login_common — 登录和注册按钮共用的业务流程
 *
 * reg=0 表示 LOGIN，reg=1 表示 REGISTER。
 * 成功后销毁登录窗口、创建主窗口，并启动 receiver_main() 后台接收线程。
 */
static void login_common(AppState *app, int reg) {
    const char *user = gtk_editable_get_text(GTK_EDITABLE(app->user_entry));
    const char *pass = gtk_editable_get_text(GTK_EDITABLE(app->pass_entry));
    char err[256];
    if (!user || !*user || !pass || !*pass) {
        gtk_label_set_text(GTK_LABEL(app->login_error), "请输入 QQ 号和密码");
        return;
    }
    snprintf(app->user, sizeof(app->user), "%s", user);
    snprintf(app->password, sizeof(app->password), "%s", pass);
    if (!auth(app, reg, err, sizeof(err))) {
        gtk_label_set_text(GTK_LABEL(app->login_error), err);
        return;
    }
    gtk_window_destroy(GTK_WINDOW(app->login_window));
    build_main(app);
    if (pthread_create(&app->recv_thread, NULL, receiver_main, app) == 0) {
        app->recv_started = 1;
    }
}

static void login_clicked(GtkButton *button, gpointer data) {
    (void)button;
    login_common((AppState *)data, 0);
}

static void register_clicked(GtkButton *button, gpointer data) {
    (void)button;
    login_common((AppState *)data, 1);
}

/*
 * build_login — 构建登录/注册窗口
 *
 * 这里只创建控件、设置 CSS class、绑定按钮事件。
 * 真正的网络认证逻辑在 login_common()/auth() 中完成。
 */
static void build_login(AppState *app) {
    GtkWidget *win = gtk_application_window_new(app->gtk_app);
    GtkWidget *root = gtk_box_new(GTK_ORIENTATION_VERTICAL, 16);
    GtkWidget *top = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
    GtkWidget *menu = gtk_button_new_with_label("☰");
    GtkWidget *close = gtk_button_new_with_label("×");
    GtkWidget *avatar = gtk_drawing_area_new();
    GtkWidget *form = gtk_box_new(GTK_ORIENTATION_VERTICAL, 18);
    GtkWidget *user = gtk_entry_new();
    GtkWidget *pass = gtk_entry_new();
    GtkWidget *opts = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 12);
    GtkWidget *auto_login = gtk_check_button_new_with_label("自动登录");
    GtkWidget *remember = gtk_check_button_new_with_label("记住密码");
    GtkWidget *agree = gtk_check_button_new_with_label("已阅读并同意服务协议和QQ隐私保护指引");
    GtkWidget *login = gtk_button_new_with_label("登录");
    GtkWidget *links = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 10);
    GtkWidget *scan = gtk_button_new_with_label("扫码登录");
    GtkWidget *reg = gtk_button_new_with_label("注册账号");
    GtkWidget *error = gtk_label_new("");

    app->login_window = win;
    app->user_entry = user;
    app->pass_entry = pass;
    app->login_error = error;

    gtk_window_set_title(GTK_WINDOW(win), "QQ");
    gtk_window_set_default_size(GTK_WINDOW(win), 476, 680);
    gtk_window_set_resizable(GTK_WINDOW(win), FALSE);
    gtk_window_set_decorated(GTK_WINDOW(win), FALSE);
    gtk_widget_add_css_class(win, "login");
    gtk_widget_add_css_class(root, "login-card");
    gtk_widget_add_css_class(top, "login-top");
    gtk_widget_set_margin_top(root, 8);
    gtk_widget_set_margin_start(root, 42);
    gtk_widget_set_margin_end(root, 42);
    gtk_widget_set_margin_bottom(root, 26);

    gtk_box_append(GTK_BOX(top), gtk_label_new(""));
    gtk_widget_set_hexpand(gtk_widget_get_first_child(top), TRUE);
    gtk_box_append(GTK_BOX(top), menu);
    gtk_box_append(GTK_BOX(top), close);

    gtk_widget_set_size_request(avatar, 120, 120);
    gtk_widget_set_halign(avatar, GTK_ALIGN_CENTER);
    gtk_drawing_area_set_draw_func(GTK_DRAWING_AREA(avatar), qq_avatar_draw, NULL, NULL);

    gtk_entry_set_placeholder_text(GTK_ENTRY(user), "QQ 账号");
    gtk_entry_set_placeholder_text(GTK_ENTRY(pass), "密码");
    gtk_entry_set_visibility(GTK_ENTRY(pass), FALSE);
    gtk_widget_add_css_class(auto_login, "login-check");
    gtk_widget_add_css_class(remember, "login-check");
    gtk_widget_add_css_class(agree, "login-check");
    gtk_check_button_set_active(GTK_CHECK_BUTTON(auto_login), TRUE);
    gtk_check_button_set_active(GTK_CHECK_BUTTON(remember), TRUE);
    gtk_check_button_set_active(GTK_CHECK_BUTTON(agree), TRUE);
    gtk_widget_set_hexpand(auto_login, TRUE);

    gtk_widget_add_css_class(login, "login-main-button");
    gtk_widget_add_css_class(scan, "login-link");
    gtk_widget_add_css_class(reg, "login-link");
    gtk_widget_add_css_class(error, "login-error");
    gtk_label_set_wrap(GTK_LABEL(error), TRUE);
    gtk_label_set_xalign(GTK_LABEL(error), 0);
    gtk_box_append(GTK_BOX(opts), auto_login);
    gtk_box_append(GTK_BOX(opts), remember);
    gtk_box_append(GTK_BOX(form), icon_entry("avatar-default-symbolic", user));
    gtk_box_append(GTK_BOX(form), icon_entry("dialog-password-symbolic", pass));
    gtk_box_append(GTK_BOX(form), opts);
    gtk_box_append(GTK_BOX(form), agree);
    gtk_box_append(GTK_BOX(form), login);
    gtk_box_append(GTK_BOX(form), error);
    gtk_widget_set_halign(links, GTK_ALIGN_CENTER);
    gtk_box_append(GTK_BOX(links), scan);
    gtk_box_append(GTK_BOX(links), gtk_label_new("|"));
    gtk_box_append(GTK_BOX(links), reg);
    gtk_box_append(GTK_BOX(form), links);

    gtk_box_append(GTK_BOX(root), top);
    gtk_box_append(GTK_BOX(root), avatar);
    gtk_box_append(GTK_BOX(root), form);
    gtk_window_set_child(GTK_WINDOW(win), root);

    g_signal_connect(close, "clicked", G_CALLBACK(close_login), app->gtk_app);
    g_signal_connect(login, "clicked", G_CALLBACK(login_clicked), app);
    g_signal_connect(reg, "clicked", G_CALLBACK(register_clicked), app);
    gtk_window_present(GTK_WINDOW(win));
}

/* tool_button — 主界面普通操作按钮的轻量封装。 */
static GtkWidget *tool_button(const char *label, const char *klass) {
    GtkWidget *button = gtk_button_new_with_label(label);
    gtk_widget_add_css_class(button, klass);
    return button;
}

/*
 * build_main — 构建登录后的主聊天界面
 *
 * 主界面采用“左侧导航 + 右侧聊天区”的布局：
 * - 左侧 profile 区显示当前账号和在线好友统计
 * - tab_bar 切换好友、群组、通知三个常用页面
 * - list_box 由 render_sidebar() 根据当前页面动态重绘
 * - 右侧 chat_box 显示聊天气泡和系统提示
 * - composer 负责文件、历史、消息输入和发送
 */
static void build_main(AppState *app) {
    GtkWidget *win = gtk_application_window_new(app->gtk_app);
    GtkWidget *root = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
    GtkWidget *side = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
    GtkWidget *profile = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 12);
    GtkWidget *profile_text = gtk_box_new(GTK_ORIENTATION_VERTICAL, 2);
    GtkWidget *profile_name = gtk_label_new(app->user);
    GtkWidget *profile_desc = gtk_label_new("");
    GtkWidget *plus_btn = gtk_menu_button_new();
    GtkWidget *plus_label = gtk_label_new("+");
    GtkWidget *popover = gtk_popover_new();
    GtkWidget *popover_box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
    GtkWidget *tab_bar = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
    GtkWidget *title = gtk_label_new("好友");
    GtkWidget *friend_tools = gtk_box_new(GTK_ORIENTATION_VERTICAL, 6);
    GtkWidget *search_box = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 6);
    GtkWidget *action_btn = tool_button("申请", "primary");
    GtkWidget *invite_entry = gtk_entry_new();
    GtkWidget *invite_btn = tool_button("邀请", "ghost");
    GtkWidget *delete_btn = tool_button("删除当前好友", "ghost");
    GtkWidget *leave_btn = tool_button("退出当前群", "ghost");
    GtkWidget *side_scroll = gtk_scrolled_window_new();
    GtkWidget *main = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
    GtkWidget *head = gtk_box_new(GTK_ORIENTATION_VERTICAL, 2);
    GtkWidget *chat_scroll = gtk_scrolled_window_new();
    GtkWidget *chat = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
    GtkWidget *composer = gtk_box_new(GTK_ORIENTATION_VERTICAL, 8);
    GtkWidget *input = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);
    GtkWidget *file = gtk_button_new();
    GtkWidget *file_icon = gtk_image_new_from_icon_name("mail-attachment-symbolic");
    GtkWidget *search = tool_button("检索", "ghost");
    GtkWidget *send = tool_button("发送", "primary");

    app->main_window = win;
    app->friend_tools = friend_tools;
    app->sidebar_title = title;
    app->profile_sub = profile_desc;
    app->friend_entry = gtk_entry_new();
    app->friend_action_button = action_btn;
    app->group_invite_entry = invite_entry;
    app->group_invite_button = invite_btn;
    app->delete_friend_button = delete_btn;
    app->leave_group_button = leave_btn;
    app->list_box = gtk_list_box_new();
    app->chat_title = gtk_label_new("请在左侧选择好友或群开始聊天");
    app->chat_subtitle = gtk_label_new("");
    app->chat_box = chat;
    app->chat_scroll = chat_scroll;
    app->message_entry = gtk_entry_new();
    app->search_entry = gtk_entry_new();
    app->search_tools = search_box;
    app->search_button = search;
    app->current_mode = 0;
    app->current_target[0] = '\0';

    gtk_window_set_title(GTK_WINDOW(win), "QQ");
    gtk_window_set_default_size(GTK_WINDOW(win), 1120, 720);
    gtk_widget_add_css_class(root, "shell");

    gtk_widget_set_size_request(side, 360, -1);
    gtk_widget_set_hexpand(side, FALSE);
    gtk_widget_set_halign(side, GTK_ALIGN_START);
    gtk_widget_add_css_class(side, "side");
    gtk_widget_add_css_class(profile, "profile");
    gtk_widget_add_css_class(profile_name, "profile-name");
    gtk_widget_add_css_class(profile_desc, "profile-sub");
    gtk_label_set_xalign(GTK_LABEL(profile_name), 0);
    gtk_label_set_xalign(GTK_LABEL(profile_desc), 0);
    gtk_box_append(GTK_BOX(profile), make_avatar(app->user, 58));
    gtk_widget_set_hexpand(profile_text, TRUE);
    gtk_box_append(GTK_BOX(profile_text), profile_name);
    gtk_box_append(GTK_BOX(profile_text), profile_desc);
    gtk_box_append(GTK_BOX(profile), profile_text);
    gtk_widget_add_css_class(plus_btn, "round-add");
    gtk_label_set_text(GTK_LABEL(plus_label), "+");
    gtk_widget_add_css_class(plus_label, "round-add-label");
    gtk_menu_button_set_child(GTK_MENU_BUTTON(plus_btn), plus_label);
    gtk_widget_add_css_class(popover_box, "plus-menu");
    gtk_box_append(GTK_BOX(popover_box), menu_action_button_new("添加好友", MENU_ADD_FRIEND, app));
    gtk_box_append(GTK_BOX(popover_box), menu_action_button_new("添加群", MENU_INVITE_GROUP, app));
    gtk_box_append(GTK_BOX(popover_box), menu_action_button_new("创建群", MENU_CREATE_GROUP, app));
    gtk_box_append(GTK_BOX(popover_box), menu_action_button_new("搜索消息", MENU_SEARCH, app));
    gtk_box_append(GTK_BOX(popover_box), menu_action_button_new("历史消息", MENU_HISTORY, app));
    gtk_box_append(GTK_BOX(popover_box), menu_action_button_new("文件记录", MENU_FILES, app));
    gtk_box_append(GTK_BOX(popover_box), menu_action_button_new("退出", MENU_QUIT, app));
    gtk_popover_set_child(GTK_POPOVER(popover), popover_box);
    gtk_menu_button_set_popover(GTK_MENU_BUTTON(plus_btn), popover);
    gtk_box_append(GTK_BOX(profile), plus_btn);

    app->nav_buttons[TAB_FRIENDS] = side_tab_button_new("好友");
    app->nav_buttons[TAB_GROUPS] = side_tab_button_new("群组");
    app->nav_buttons[TAB_MESSAGES] = side_tab_button_new("通知");
    g_object_set_data(G_OBJECT(app->nav_buttons[TAB_FRIENDS]), "tab", GINT_TO_POINTER(TAB_FRIENDS));
    g_object_set_data(G_OBJECT(app->nav_buttons[TAB_GROUPS]), "tab", GINT_TO_POINTER(TAB_GROUPS));
    g_object_set_data(G_OBJECT(app->nav_buttons[TAB_MESSAGES]), "tab", GINT_TO_POINTER(TAB_MESSAGES));
    g_signal_connect(app->nav_buttons[TAB_FRIENDS], "clicked", G_CALLBACK(nav_clicked), app);
    g_signal_connect(app->nav_buttons[TAB_GROUPS], "clicked", G_CALLBACK(nav_clicked), app);
    g_signal_connect(app->nav_buttons[TAB_MESSAGES], "clicked", G_CALLBACK(nav_clicked), app);
    gtk_widget_add_css_class(tab_bar, "side-tabs");
    gtk_box_append(GTK_BOX(tab_bar), app->nav_buttons[TAB_FRIENDS]);
    gtk_box_append(GTK_BOX(tab_bar), app->nav_buttons[TAB_GROUPS]);
    gtk_box_append(GTK_BOX(tab_bar), app->nav_buttons[TAB_MESSAGES]);

    gtk_widget_add_css_class(title, "side-title");
    gtk_label_set_xalign(GTK_LABEL(title), 0);
    gtk_entry_set_placeholder_text(GTK_ENTRY(app->search_entry), "检索历史消息");
    gtk_editable_set_width_chars(GTK_EDITABLE(app->search_entry), 12);
    gtk_editable_set_max_width_chars(GTK_EDITABLE(app->search_entry), 18);
    gtk_widget_set_hexpand(app->search_entry, TRUE);
    gtk_widget_set_hexpand(search, FALSE);
    gtk_box_append(GTK_BOX(search_box), app->search_entry);
    gtk_box_append(GTK_BOX(search_box), search);
    gtk_box_append(GTK_BOX(friend_tools), search_box);
    gtk_editable_set_text(GTK_EDITABLE(app->friend_entry), "");
    gtk_entry_set_placeholder_text(GTK_ENTRY(app->friend_entry), "搜索或添加 QQ 号");
    gtk_editable_set_width_chars(GTK_EDITABLE(app->friend_entry), 15);
    gtk_editable_set_max_width_chars(GTK_EDITABLE(app->friend_entry), 20);
    gtk_widget_set_hexpand(app->friend_entry, TRUE);
    gtk_box_append(GTK_BOX(friend_tools), app->friend_entry);
    gtk_box_append(GTK_BOX(friend_tools), action_btn);
    gtk_widget_set_hexpand(invite_entry, TRUE);
    gtk_editable_set_width_chars(GTK_EDITABLE(invite_entry), 15);
    gtk_editable_set_max_width_chars(GTK_EDITABLE(invite_entry), 20);
    gtk_box_append(GTK_BOX(friend_tools), invite_entry);
    gtk_box_append(GTK_BOX(friend_tools), invite_btn);
    gtk_box_append(GTK_BOX(friend_tools), delete_btn);
    gtk_box_append(GTK_BOX(friend_tools), leave_btn);
    gtk_widget_set_margin_start(friend_tools, 16);
    gtk_widget_set_margin_end(friend_tools, 16);
    gtk_widget_set_margin_bottom(friend_tools, 12);
    gtk_scrolled_window_set_child(GTK_SCROLLED_WINDOW(side_scroll), app->list_box);
    gtk_box_append(GTK_BOX(side), profile);
    gtk_box_append(GTK_BOX(side), tab_bar);
    gtk_box_append(GTK_BOX(side), title);
    gtk_box_append(GTK_BOX(side), friend_tools);
    gtk_box_append(GTK_BOX(side), side_scroll);
    gtk_widget_set_vexpand(side_scroll, TRUE);

    gtk_widget_add_css_class(main, "chat-main");
    gtk_widget_set_hexpand(main, TRUE);
    gtk_widget_add_css_class(head, "chat-head");
    gtk_widget_add_css_class(app->chat_title, "chat-title");
    gtk_widget_add_css_class(app->chat_subtitle, "chat-subtitle");
    gtk_label_set_xalign(GTK_LABEL(app->chat_title), 0);
    gtk_label_set_xalign(GTK_LABEL(app->chat_subtitle), 0);
    gtk_box_append(GTK_BOX(head), app->chat_title);
    gtk_box_append(GTK_BOX(head), app->chat_subtitle);
    gtk_widget_add_css_class(chat_scroll, "chat-area");
    gtk_widget_add_css_class(chat, "chat-area");
    gtk_scrolled_window_set_child(GTK_SCROLLED_WINDOW(chat_scroll), chat);
    gtk_widget_add_css_class(composer, "composer");
    gtk_widget_add_css_class(file, "icon-tool");
    gtk_image_set_pixel_size(GTK_IMAGE(file_icon), 22);
    gtk_button_set_child(GTK_BUTTON(file), file_icon);
    gtk_entry_set_placeholder_text(GTK_ENTRY(app->message_entry), "输入消息，Enter 或点击发送");
    gtk_widget_set_hexpand(app->message_entry, TRUE);
    gtk_box_append(GTK_BOX(input), app->message_entry);
    gtk_box_append(GTK_BOX(input), file);
    gtk_box_append(GTK_BOX(input), send);
    gtk_box_append(GTK_BOX(composer), input);
    gtk_box_append(GTK_BOX(main), head);
    gtk_box_append(GTK_BOX(main), chat_scroll);
    gtk_widget_set_vexpand(chat_scroll, TRUE);
    gtk_box_append(GTK_BOX(main), composer);

    gtk_box_append(GTK_BOX(root), side);
    gtk_box_append(GTK_BOX(root), main);
    gtk_window_set_child(GTK_WINDOW(win), root);
    g_signal_connect(app->list_box, "row-activated", G_CALLBACK(row_activated), app);
    g_signal_connect(action_btn, "clicked", G_CALLBACK(add_friend_clicked), app);
    g_signal_connect(invite_btn, "clicked", G_CALLBACK(invite_group_clicked), app);
    g_signal_connect(delete_btn, "clicked", G_CALLBACK(delete_friend_clicked), app);
    g_signal_connect(leave_btn, "clicked", G_CALLBACK(leave_group_clicked), app);
    g_signal_connect(send, "clicked", G_CALLBACK(send_clicked), app);
    g_signal_connect(app->message_entry, "activate", G_CALLBACK(message_activate), app);
    g_signal_connect(file, "clicked", G_CALLBACK(send_file_clicked), app);
    g_signal_connect(search, "clicked", G_CALLBACK(search_clicked), app);
    g_signal_connect(app->search_entry, "activate", G_CALLBACK(search_clicked), app);
    set_tab(app, TAB_FRIENDS);
    {
        UiCounts *ui = g_new0(UiCounts, 1);
        ui->app = app;
        ui->friends = 0;
        ui->online_friends = 0;
        ui_update_counts(ui);
    }
    append_notice(app, "已登录 %s，连接 %s:%d，P2P 文件端口 %d", app->user, app->host, app->port, app->file_port);
    gtk_window_present(GTK_WINDOW(win));
}

/*
 * activate — GTK 应用激活回调
 *
 * GTKApplication 启动后先注册 CSS，再显示登录窗口。
 * 主界面不会立即创建，只有登录/注册成功后才调用 build_main()。
 */
static void activate(GtkApplication *gtk_app, gpointer data) {
    AppState *app = (AppState *)data;
    app->gtk_app = gtk_app;
    apply_css();
    build_login(app);
}

/*
 * main — 客户端启动入口
 *
 * 启动顺序：
 *   1. 初始化 AppState、GLib 数组、mutex/cond
 *   2. 设置中文 locale 和输入法环境，保证 GTK 输入框可输入中文
 *   3. 启动 P2P 文件监听线程，取得 file_port
 *   4. 创建 GTKApplication，进入 GTK 主循环
 *   5. 主循环退出后发送 QUIT，关闭 socket，等待后台线程结束
 */
int main(int argc, char **argv) {
    AppState app;
    int status;
    memset(&app, 0, sizeof(app));
    app.sockfd = -1;
    app.file_listen_fd = -1;
    snprintf(app.host, sizeof(app.host), "%s", argc > 1 ? argv[1] : "127.0.0.1");
    app.port = argc > 2 ? atoi(argv[2]) : QQ_DEFAULT_PORT;
    init_download_dir(&app);
    app.friends = g_ptr_array_new_with_free_func(g_free);
    app.groups = g_ptr_array_new_with_free_func(g_free);
    app.files = g_ptr_array_new_with_free_func(g_free);
    app.requests = g_ptr_array_new_with_free_func(g_free);
    pthread_mutex_init(&app.send_mutex, NULL);
    pthread_mutex_init(&app.peer_mutex, NULL);
    pthread_cond_init(&app.peer_cond, NULL);
    signal(SIGPIPE, SIG_IGN);

    /*
     * 中文输入支持：
     * 如果外部环境没有设置 LANG/LC_CTYPE，则默认使用 UTF-8。
     * 如果检测到 fcitx/ibus，就设置 GTK_IM_MODULE，提升 openEuler 桌面中文输入体验。
     */
    if (!g_getenv("LANG")) {
        g_setenv("LANG", "zh_CN.UTF-8", FALSE);
    }
    if (!g_getenv("LC_CTYPE")) {
        g_setenv("LC_CTYPE", "zh_CN.UTF-8", FALSE);
    }
    if (!g_getenv("GTK_IM_MODULE")) {
        const char *xmods = g_getenv("XMODIFIERS");
        const char *qt_im = g_getenv("QT_IM_MODULE");
        if ((xmods && strstr(xmods, "fcitx")) || (qt_im && strcmp(qt_im, "fcitx") == 0)) {
            g_setenv("GTK_IM_MODULE", "fcitx", FALSE);
        } else if ((xmods && strstr(xmods, "ibus")) || (qt_im && strcmp(qt_im, "ibus") == 0)) {
            g_setenv("GTK_IM_MODULE", "ibus", FALSE);
        } else {
            char *fcitx = g_find_program_in_path("fcitx5");
            char *ibus = g_find_program_in_path("ibus-daemon");
            if (fcitx) {
                g_setenv("GTK_IM_MODULE", "fcitx", FALSE);
            } else if (ibus) {
                g_setenv("GTK_IM_MODULE", "ibus", FALSE);
            }
            g_free(fcitx);
            g_free(ibus);
        }
    }
    setlocale(LC_ALL, "");

    /*
     * P2P 文件监听先于登录启动。
     * 登录时 auth() 会把 app.file_port 一并发送给服务端，
     * 这样其他客户端才能通过 PEER_INFO 查询到本客户端的文件接收端口。
     */
    if (setup_file_listener(&app) < 0) {
        fprintf(stderr, "failed to start p2p file listener\n");
    } else {
        if (pthread_create(&app.file_thread, NULL, file_listener_main, &app) == 0) {
            app.file_thread_started = 1;
        }
    }

    /*
     * 使用 Cairo/软件渲染，规避部分虚拟机或课程实验环境里 OpenGL/Mesa 驱动警告。
     * 这只影响 GTK 渲染后端，不影响聊天协议和文件传输逻辑。
     */
    g_setenv("GSK_RENDERER", "cairo", TRUE);
    g_setenv("LIBGL_ALWAYS_SOFTWARE", "1", TRUE);
    app.gtk_app = gtk_application_new("local.qq.gtk4", G_APPLICATION_NON_UNIQUE);
    g_object_set_data(G_OBJECT(app.gtk_app), "state", &app);
    g_signal_connect(app.gtk_app, "activate", G_CALLBACK(activate), &app);
    status = g_application_run(G_APPLICATION(app.gtk_app), argc, argv);
    app.shutting_down = 1;
    /* 优雅退出：通知服务器、关闭控制连接，再停止接收线程和 P2P 文件监听线程。 */
    if (app.sockfd >= 0) {
        send_cmd(&app, "QUIT");
        shutdown(app.sockfd, SHUT_RDWR);
        close(app.sockfd);
    }
    if (app.recv_started) {
        pthread_join(app.recv_thread, NULL);
    }
    if (app.file_listen_fd >= 0) {
        shutdown(app.file_listen_fd, SHUT_RDWR);
        close(app.file_listen_fd);
    }
    if (app.file_thread_started) {
        pthread_join(app.file_thread, NULL);
    }
    g_ptr_array_free(app.friends, TRUE);
    g_ptr_array_free(app.groups, TRUE);
    g_ptr_array_free(app.files, TRUE);
    g_ptr_array_free(app.requests, TRUE);
    pthread_mutex_destroy(&app.send_mutex);
    pthread_mutex_destroy(&app.peer_mutex);
    pthread_cond_destroy(&app.peer_cond);
    g_object_unref(app.gtk_app);
    return status;
}
