package gms.cims.bridge;

import io.confluent.kafka.serializers.subject.TopicNameStrategy;

public class SchemaRegistryName {
    public static String Get(String subject, Object topicNameStrategy){
        return ((TopicNameStrategy)topicNameStrategy).subjectName(subject,false, null);
    }
}
