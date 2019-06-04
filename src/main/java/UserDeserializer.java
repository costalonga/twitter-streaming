//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
//import org.apache.kafka.common.serialization.Deserializer;
//
//import java.io.IOException;
//import java.util.Map;
//
//public class UserDeserializer implements Deserializer<User> {
//    ObjectMapper mapper = new ObjectMapper();
//
//    public UserDeserializer() {
//
//
//        JavaTimeModule javaTimeModule = new JavaTimeModule();
//        mapper.registerModule(javaTimeModule);
//
//
//    }
//
//    @Override
//    public User deserialize(String key, byte[] value) {
//        User user = null;
//        try {
//            user = mapper.readValue(value, User.class);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return user;
//    }
//
//    @Override
//    public void configure(Map<String, ?> map, boolean b) {
//    }
//
//    @Override
//    public void close() {
//    }
//}