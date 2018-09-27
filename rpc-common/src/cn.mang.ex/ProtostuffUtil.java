package cn.mang.ex;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProtostuffUtil {

    private static Map<Class<?>, Schema<?>> cachedSchema = new ConcurrentHashMap<Class<?>, Schema<?>>();

    private static Objenesis objenesis = new ObjenesisStd(true);

    private ProtostuffUtil() {
    }

    private static <T> Schema<T> getSchema(Class<T> tClass) {
        Schema<T> schema = (Schema<T>) cachedSchema.get(tClass);
        if (schema == null) {
            schema = RuntimeSchema.createFrom(tClass);
            if (schema != null)
                cachedSchema.put(tClass, schema);
        }

        return schema;

    }

    public static <T> byte[] serialize(T obj) {
        Class<T> aClass = (Class<T>) obj.getClass();
        LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        Schema<T> schema = getSchema(aClass);
        try {
            return ProtostuffIOUtil.toByteArray(obj, schema, buffer);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        } finally {
            buffer.clear();
        }
    }


    public static <T> T deserialize(byte[] data, Class<T> cls) {
        try {
            T message = (T) objenesis.newInstance(cls);//实例化
            Schema<T> schema = getSchema(cls);//获取类的schema
            ProtostuffIOUtil.mergeFrom(data, message, schema);
            return message;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }


}
