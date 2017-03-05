// https://tokio.rs/docs/going-deeper/multiplex/

//extern crate byteorder;

use std::io;

//use byteorder::BigEndian;
use byteorder::{BigEndian, ByteOrder};

use tokio_core::io::{Io, Codec, EasyBuf, Framed};
use tokio_proto::multiplex::{ServerProto, RequestId};

pub struct LineCodec;




impl Codec for LineCodec {
    type In = (RequestId, Vec<u8>);
    type Out = (RequestId, Vec<u8>);

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<(RequestId, Vec<u8>)>, io::Error> {
        if buf.len() < 20 {
            return Ok(None);
        }

        let id = BigEndian::read_u32(&buf.drain_to(4).as_ref()[0..4]);
        let uuid: Vec<u8> = buf.drain_to(16).as_ref()[0..16].to_vec();

        Ok(Some((id as RequestId, uuid)))
    }

    fn encode(&mut self, msg: (RequestId, Vec<u8>), buf: &mut Vec<u8>) -> io::Result<()> {
        let (id, msg) = msg;

        let mut encoded_id = [0; 4];
        BigEndian::write_u32(&mut encoded_id, id as u32);

        let mut len = [0; 4];
        BigEndian::write_u32(&mut len, msg.len() as u32);

        buf.extend(&encoded_id);
        buf.extend(&len);
        buf.extend(msg);

        Ok(())
    }
}


pub struct LineProto;

impl<T: Io + 'static> ServerProto<T> for LineProto {
    type Request = Vec<u8>;
    type Response = Vec<u8>;

    // `Framed<T, LineCodec>` is the return value
    // of `io.framed(LineCodec)`
    type Transport = Framed<T, LineCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(LineCodec))
    }
}
