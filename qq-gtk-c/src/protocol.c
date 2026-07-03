/*
 * protocol.c — QQ 聊天系统协议工具函数实现
 *
 * ======== 本文件职责 ========
 * 这个文件是客户端和服务端共用的底层协议工具库。
 * 它不涉及任何业务命令（如 LOGIN、MSGP、MSGG 等），
 * 只负责三件事：
 *   1. 可靠的网络收发（处理分包、信号中断）
 *   2. 按行分隔的消息帧（文本协议的基础）
 *   3. 百分号转义/反转义（保证控制字符不破坏协议格式）
 *
 * ======== 为什么单独抽出 protocol 层 ========
 * 将协议层和业务层分离的好处：
 *   1. 客户端和服务端共享同一套代码，修改协议只需改一处
 *   2. 答辩时可以先讲底层协议，再讲上层业务，结构清晰
 *   3. 方便单独对协议层做单元测试
 */

#include "protocol.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/*
 * qq_send_all — 可靠地发送指定长度的全部数据
 *
 * 普通的 send() 调用可能出现"短写"（short write），即一次 send 只发送了
 * 部分数据。这在 socket 缓冲区满或网络拥塞时很常见。本函数循环调用 send()
 * 直到所有数据发送完毕或发生不可恢复的错误。
 *
 * 同时处理了 EINTR 信号中断：当进程收到信号时 send() 可能返回 -1 并设
 * errno=EINTR，这时只需要重试即可，不算真正的错误。
 */
int qq_send_all(int fd, const void *buf, size_t len) {
    const char *p = (const char *)buf;
    while (len > 0) {
        ssize_t n = send(fd, p, len, 0);  /* 尝试发送剩余数据 */
        if (n < 0) {
            if (errno == EINTR) {
                /* 被信号中断，不是错误，重试即可 */
                continue;
            }
            return -1;  /* 真正的网络错误 */
        }
        if (n == 0) {
            /* send 返回 0 表示连接已关闭，无法继续发送 */
            return -1;
        }
        p += n;          /* 指针前移 */
        len -= (size_t)n; /* 减少剩余字节数 */
    }
    return 0;
}

/*
 * qq_send_line — 发送一行以换行符结尾的协议消息
 *
 * 在数据内容之后追加 '\n' 作为消息结束标记。
 * 接收方通过 qq_recv_line() 读取到 '\n' 即认为一条消息结束。
 */
int qq_send_line(int fd, const char *line) {
    /* 先发送消息正文 */
    if (qq_send_all(fd, line, strlen(line)) < 0) {
        return -1;
    }
    /* 再发送换行符作为消息结束标记 */
    return qq_send_all(fd, "\n", 1);
}

/*
 * qq_recv_line — 从 socket 接收一行以换行符结尾的协议消息
 *
 * 采用逐字节读取的方式，因为：
 *   1. 文本协议以 '\n' 作为帧分隔符，逐字节读能精确控制边界
 *   2. TCP 是字节流，无法保证一次 recv 恰好收到一条完整消息
 *   3. 避免读过头（把下一条消息的部分数据也读进来）
 *
 * 缺点：逐字节读效率较低，但对于聊天应用来说消息频率不高，影响可忽略。
 *
 * 同时处理了：
 *   - EINTR 信号中断 → 重试
 *   - recv 返回 0 → 连接关闭
 *   - 行尾的 '\r' → 自动去除（兼容 CRLF 格式）
 */
ssize_t qq_recv_line(int fd, char *buf, size_t cap) {
    size_t used = 0;
    if (cap == 0) {
        return -1;  /* 缓冲区大小为 0，无法接收 */
    }

    /* 逐字节读取直到遇到 '\n'、缓冲区满、或连接关闭 */
    while (used + 1 < cap) {  /* +1 留给结尾的 '\0' */
        char ch;
        ssize_t n = recv(fd, &ch, 1, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;  /* 被信号中断，重试 */
            }
            return -1;  /* 真正的网络错误 */
        }
        if (n == 0) {
            /* recv 返回 0 表示连接已关闭（对端发送了 FIN） */
            if (used == 0) {
                return 0;  /* 还没读到数据就关闭了 */
            }
            break;  /* 已读到部分数据，当作最后一行处理 */
        }
        if (ch == '\n') {
            break;  /* 遇到消息结束标记 */
        }
        buf[used++] = ch;  /* 保存字符 */
    }
    buf[used] = '\0';  /* 添加 C 字符串结尾 */

    /* 兼容 Windows 风格的 CRLF：去除行尾的 '\r' */
    if (used > 0 && buf[used - 1] == '\r') {
        buf[used - 1] = '\0';
        used--;
    }
    return (ssize_t)used;  /* 返回接收到的字节数（不含 '\n' 和 '\0'） */
}

/*
 * ======== 百分号转义工具 ========
 *
 * 为什么需要转义？
 * 协议使用 TAB 分隔字段、换行分隔消息。如果聊天内容中包含 TAB 或换行符，
 * 不转义就会破坏协议格式，导致解析错误。
 *
 * 解决方案：对正文中的特殊字符做百分号编码（类似 URL 编码），
 * 接收方还原即可。这样聊天内容可以包含任意字符。
 */

/* 判断字符是否为合法的十六进制数字（0-9, A-F, a-f） */
static int is_hex(char c) {
    return isxdigit((unsigned char)c);
}

/* 将单个十六进制字符转换为对应的数值（0-15） */
static int from_hex(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return 0;  /* 非十六进制字符，返回 0 */
}

/*
 * qq_escape — 对字符串进行百分号转义编码
 *
 * 算法：
 *   1. 第一遍扫描：统计需要转义的特殊字符数量，计算输出缓冲区大小
 *   2. 分配足够的内存
 *   3. 第二遍扫描：逐字符复制，遇到 %/\t/\n/\r 时替换为 %XX 格式
 *
 * 两遍扫描的好处是只需要一次内存分配，避免多次 realloc。
 *
 * 转义映射：
 *   '%' → "%25"
 *   '\t' → "%09"
 *   '\n' → "%0A"
 *   '\r' → "%0D"
 */
char *qq_escape(const char *src) {
    static const char *hex = "0123456789ABCDEF";  /* 十六进制字符表 */
    size_t extra = 0;    /* 需要额外分配的字节数 */
    const unsigned char *p;
    char *out;
    char *q;

    if (!src) {
        src = "";  /* NULL 字符串当作空字符串处理 */
    }

    /* 第一遍：统计需要转义的字符数量，每个特殊字符需要多 2 字节（% + 两位十六进制） */
    for (p = (const unsigned char *)src; *p; p++) {
        if (*p == '%' || *p == '\t' || *p == '\n' || *p == '\r') {
            extra += 2;  /* 1 字节变成 3 字节 (%XX)，净增 2 */
        }
    }

    /* 分配输出缓冲区：原始长度 + 额外空间 + 结尾 '\0' */
    out = (char *)malloc(strlen(src) + extra + 1);
    if (!out) {
        return NULL;  /* 内存分配失败 */
    }

    /* 第二遍：逐字符复制并转义 */
    q = out;
    for (p = (const unsigned char *)src; *p; p++) {
        if (*p == '%' || *p == '\t' || *p == '\n' || *p == '\r') {
            /* 特殊字符替换为 %XX */
            *q++ = '%';
            *q++ = hex[*p >> 4];      /* 高 4 位 */
            *q++ = hex[*p & 0x0f];    /* 低 4 位 */
        } else {
            *q++ = (char)*p;  /* 普通字符直接复制 */
        }
    }
    *q = '\0';
    return out;
}

/*
 * qq_unescape — 对百分号转义的字符串进行解码还原
 *
 * 算法：
 *   1. 分配与输入等长的缓冲区（解码后不会比输入长）
 *   2. 逐字符扫描，遇到 '%' 后紧跟两位十六进制数则还原为一个字节
 *   3. 非转义序列的 '%' 保留原样（兼容性处理）
 *
 * 示例：
 *   "Hello%09World" → "Hello\tWorld"
 *   "100%25"        → "100%"
 */
char *qq_unescape(const char *src) {
    char *out;
    char *q;
    const char *p;

    if (!src) {
        src = "";
    }

    /* 解码后的长度不会超过原始长度 */
    out = (char *)malloc(strlen(src) + 1);
    if (!out) {
        return NULL;
    }

    q = out;
    for (p = src; *p; p++) {
        /* 检测 %XX 转义序列：% 后紧跟两个合法的十六进制字符 */
        if (*p == '%' && is_hex(p[1]) && is_hex(p[2])) {
            /* 将两位十六进制数合并为一个字节 */
            *q++ = (char)((from_hex(p[1]) << 4) | from_hex(p[2]));
            p += 2;  /* 跳过已处理的两个十六进制字符 */
        } else {
            *q++ = *p;  /* 普通字符直接复制 */
        }
    }
    *q = '\0';
    return out;
}

/*
 * qq_basename_dup — 从文件路径中提取文件名
 *
 * 支持 Unix 路径分隔符 '/' 和 Windows 路径分隔符 '\'
 * 使用 strrchr 从后往前找，取最后出现的分隔符之后的部分。
 *
 * 示例：
 *   "/home/user/file.txt" → "file.txt"
 *   "C:\Users\doc.pdf"    → "doc.pdf"
 *   NULL 或 "" 或 "." 或 ".." → "file.bin"（默认文件名）
 */
char *qq_basename_dup(const char *path) {
    const char *slash;
    const char *backslash;
    const char *base;

    if (!path || !*path) {
        return strdup("file.bin");
    }

    /* 同时查找 Unix 和 Windows 风格的分隔符 */
    slash = strrchr(path, '/');
    backslash = strrchr(path, '\\');
    /* 取靠后的那个作为真正的分隔符位置 */
    base = slash > backslash ? slash : backslash;
    base = base ? base + 1 : path;  /* 跳过分隔符 */

    /* 处理特殊情况：空文件名或 "." ".." */
    if (!*base || strcmp(base, ".") == 0 || strcmp(base, "..") == 0) {
        return strdup("file.bin");
    }
    return strdup(base);
}

/*
 * qq_trim_newline — 去除字符串末尾的换行符
 *
 * 原地修改字符串，去除末尾的所有 '\n' 和 '\r' 字符。
 * fgets() 和 qq_recv_line() 可能保留行尾换行符，
 * 在需要纯文本内容时调用此函数。
 */
void qq_trim_newline(char *s) {
    size_t n;
    if (!s) {
        return;
    }
    n = strlen(s);
    while (n > 0 && (s[n - 1] == '\n' || s[n - 1] == '\r')) {
        s[--n] = '\0';  /* 将末尾的换行符逐个替换为 '\0' */
    }
}
