/*
 * protocol.h — QQ 聊天系统通信协议定义
 *
 * ======== 协议设计 ========
 * 本项目采用基于 TCP 的文本协议，以换行符 '\n' 分隔每条消息，
 * 消息内部字段使用 TAB 字符 '\t' 分隔。
 *
 * 协议格式示例：
 *   LOGIN\talice\tAliceDisplay\t12345\tpassword123
 *   MSGP\tbob\tHello World
 *   MSGG\tgroup1\t群聊消息内容
 *
 * ======== 为什么用文本协议 ========
 * 1. 易于调试：可以直接用 telnet/netcat 测试，也可以直接查看数据文件
 * 2. 课程设计友好：答辩时能清晰展示协议格式和字段含义
 * 3. 跨平台：不依赖二进制序列化库，纯 C 标准库即可
 *
 * ======== 转义机制 ========
 * 因为 TAB 和换行符是协议的控制字符，消息正文中不能直接出现。
 * 协议使用百分号转义（类似 URL 编码）：
 *   % → %25
 *   \t → %09
 *   \n → %0A
 *   \r → %0D
 * 发送前调用 qq_escape() 转义，接收后调用 qq_unescape() 还原。
 *
 * ======== 数据持久化 ========
 * 服务端使用 data 目录下的 TSV 文件存储数据，与协议格式一致，
 * 同样使用 TAB 分隔字段、换行分隔记录、百分号转义。
 */

#ifndef QQ_PROTOCOL_H
#define QQ_PROTOCOL_H

#include <stddef.h>
#include <sys/types.h>

/* 单条协议消息的最大长度（包含所有字段和转义后的正文） */
#define QQ_MAX_LINE 8192

/* 用户名/群名/昵称等标识符的最大长度 */
#define QQ_MAX_NAME 64

/* 服务端默认监听端口 */
#define QQ_DEFAULT_PORT 9090

/*
 * ======== 基础网络 I/O ========
 */

/*
 * qq_send_all — 可靠发送指定长度的数据
 * @fd:   已连接的 socket 文件描述符
 * @buf:  待发送数据的缓冲区指针
 * @len:  待发送数据的字节数
 *
 * 返回值：成功返回 0，失败返回 -1
 *
 * 说明：封装了 send() 系统调用，处理了 EINTR 信号中断和短写（short write）
 *       的情况。在网络不稳定的情况下会循环发送直到所有数据发送完毕。
 *       这是构建所有上层发送函数的基础。
 */
int qq_send_all(int fd, const void *buf, size_t len);

/*
 * qq_send_line — 发送一行协议消息
 * @fd:   已连接的 socket 文件描述符
 * @line: 要发送的字符串（不需要包含换行符）
 *
 * 返回值：成功返回 0，失败返回 -1
 *
 * 说明：先发送字符串内容，再追加一个 '\n' 作为消息结束标记。
 *       这是服务端和客户端发送协议命令的主要接口。
 */
int qq_send_line(int fd, const char *line);

/*
 * qq_recv_line — 从 socket 接收一行协议消息
 * @fd:  已连接的 socket 文件描述符
 * @buf: 接收缓冲区
 * @cap: 缓冲区容量（最大字节数，包含结尾的 '\0'）
 *
 * 返回值：
 *   > 0  — 接收到的字节数（不含 '\n' 和结尾 '\0'）
 *   = 0  — 连接已关闭（对端发送了 FIN）
 *   = -1 — 接收错误
 *
 * 说明：逐字节读取直到遇到 '\n' 或缓冲区满。
 *       自动去除行尾的 '\r'（兼容 Windows 风格的 CRLF）。
 *       这是构建所有上层接收函数的基础。
 */
ssize_t qq_recv_line(int fd, char *buf, size_t cap);

/*
 * ======== 百分号转义工具 ========
 */

/*
 * qq_escape — 对字符串进行百分号转义
 * @src: 原始字符串（可以为 NULL，当作空字符串处理）
 *
 * 返回值：转义后的新字符串（调用者需要用 free() 释放），内存分配失败返回 NULL
 *
 * 转义规则：% → %25, \t → %09, \n → %0A, \r → %0D
 *
 * 说明：用于在消息正文、昵称、文件名等可能包含控制字符的字段发送前进行编码。
 *       服务端存储到 TSV 文件时也需要转义。
 */
char *qq_escape(const char *src);

/*
 * qq_unescape — 对百分号转义的字符串进行解码还原
 * @src: 转义后的字符串（可以为 NULL，当作空字符串处理）
 *
 * 返回值：还原后的新字符串（调用者需要用 free() 释放），内存分配失败返回 NULL
 *
 * 说明：将 %XX 格式的十六进制编码还原为原始字符。
 *       客户端收到消息、服务端从 TSV 文件读取记录后调用此函数还原。
 */
char *qq_unescape(const char *src);

/*
 * qq_basename_dup — 从文件路径中提取文件名
 * @path: 文件路径（支持 Unix 风格 '/' 和 Windows 风格 '\\' 分隔符）
 *
 * 返回值：文件名的副本（调用者需要用 free() 释放）
 *
 * 说明：用于文件传输时从完整路径中提取文件名显示给用户。
 *       如果路径为空或只包含 "." / ".."，则返回默认文件名 "file.bin"。
 */
char *qq_basename_dup(const char *path);

/*
 * qq_trim_newline — 去除字符串末尾的换行符
 * @s: 待处理的字符串（原地修改）
 *
 * 说明：去除字符串末尾的 '\n' 和 '\r' 字符。
 *       从 TSV 文件读取的行或网络接收的行通常带有换行符，
 *       调用此函数可以方便地去除。
 */
void qq_trim_newline(char *s);

#endif
