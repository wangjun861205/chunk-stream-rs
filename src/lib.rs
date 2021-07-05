use bytes::{BufMut, Bytes, BytesMut};
use futures::{
    task::{Context, Poll},
    Stream, StreamExt,
};
use std::pin::Pin;

type Error = Box<dyn std::error::Error + Send + Sync>;
type BytesStream = Box<dyn Stream<Item = Result<Bytes, Error>> + Send + Sync + Unpin>;
pub struct ChunkStream {
    buf: BytesMut,
    cap: usize,
    inner: BytesStream,
}

impl ChunkStream {
    pub fn new(stream: BytesStream) -> Self {
        Self {
            buf: BytesMut::with_capacity(8 * 1024),
            cap: 8 * 1024,
            inner: stream,
        }
    }
    pub fn with_capacity(stream: BytesStream, size: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(size),
            cap: size,
            inner: stream,
        }
    }
}

impl Stream for ChunkStream {
    type Item = Result<Bytes, Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let cap = self.cap;
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(res)) => match res {
                Ok(bs) => {
                    self.buf.put_slice(&bs);
                    if self.buf.len() >= cap {
                        return Poll::Ready(Some(Ok(self.buf.split_to(cap).freeze())));
                    } else {
                        return Poll::Pending;
                    }
                }
                Err(e) => {
                    return Poll::Ready(Some(Err(e.into())));
                }
            },
            Poll::Ready(None) => {
                if self.buf.len() > 0 {
                    if self.buf.len() >= cap {
                        return Poll::Ready(Some(Ok(self.buf.split_to(cap).freeze())));
                    } else {
                        return Poll::Ready(Some(Ok(self.buf.split().freeze())));
                    }
                } else {
                    return Poll::Ready(None);
                }
            }
            Poll::Pending => {
                return Poll::Pending;
            }
        }
    }
}

#[tokio::test]
async fn test_chunk_stream() {
    use futures::stream::iter;
    let mut l = Vec::new();
    for _ in 0..3 {
        let mut buf = BytesMut::new();
        for j in 0..255 {
            buf.put_u8(j as u8);
        }
        l.push(buf.freeze());
    }
    let s = iter(l).map(|v| Ok::<Bytes, Error>(v));
    let mut cs = ChunkStream::with_capacity(Box::new(s), 100);
    while let Some(Ok(bs)) = cs.next().await {
        println!("{}", bs.len());
    }
}
