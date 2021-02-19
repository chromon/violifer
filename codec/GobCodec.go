package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

// gob 编解码并读写方式，实现 Codec 接口
type GobCodec struct {
	// TCP 或者 Unix 建立 socket 时得到的链接实例
	conn io.ReadWriteCloser
	// 使用带缓冲 Writer 提升性能
	buf *bufio.Writer
	// gob 解码
	dec *gob.Decoder
	// gob 编码
	enc *gob.Encoder
}

var _ Codec = (*GobCodec)(nil)

func NewGobCodec(conn io.ReadWriteCloser) Codec {
	// 创建一个具有默认大小缓冲、写入 conn 的 *Writer
	buf := bufio.NewWriter(conn)

	return &GobCodec{
		conn: conn,
		buf:  buf,
		dec:  gob.NewDecoder(conn), // 返回从 conn 中读取数据的 *Decoder
		enc:  gob.NewEncoder(buf), // 返回将编码后数据写入 buf 的 *Encoder
	}
}

func (c *GobCodec) ReadHeader(h *Header) error {
	// 从输入流中读取下一个值并存储到 h 中
	return c.dec.Decode(h)
}

func (c *GobCodec) ReadBody(body interface{}) error {
	// 从输入流中读取下一个值并存储到 body 中
	return c.dec.Decode(body)
}

func (c *GobCodec) Write(h *Header, body interface{}) (err error) {
	defer func() {
		// 将缓冲中的数据写入下层的 io.Writer 接口
		_ = c.buf.Flush()
		if err != nil {
			_ = c.Close()
		}
	}()

	// 将 h 编码后发送到 buf 中
	if err := c.enc.Encode(h); err != nil {
		log.Println("rpc codec: gob error encoding header:", err)
		return err
	}
	// 将 body 编码后发送到 buf 中
	if err := c.enc.Encode(body); err != nil {
		log.Println("rpc codec: gob error encoding body：", err)
		return err
	}
	return
}

// 关闭连接
func (c *GobCodec) Close() error {
	return c.conn.Close()
}