package cn.mang.ex;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class RpcDecoder extends ByteToMessageDecoder {

    private Class genericClass;

    public RpcDecoder(Class genericClass) {
        this.genericClass = genericClass;
    }

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List list) throws Exception {
        if (byteBuf.readableBytes() < 4) {
            return;
        }

        internalBuffer().markReaderIndex();
        int length = byteBuf.readInt();
        if (length < 0) {
            channelHandlerContext.close();
        }
        if (byteBuf.readableBytes() < length) {
            byteBuf.resetReaderIndex();
        }

        byte[] data = new byte[length];
        byteBuf.readBytes(data);

        Object deserialize = ProtostuffUtil.deserialize(data, genericClass);

        list.add(deserialize);

    }
}
