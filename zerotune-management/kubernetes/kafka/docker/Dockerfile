FROM alpine:3.14
COPY ./kafka-producer ./kafka-producer
RUN apk add --no-cache gradle openjdk11
RUN gradle build -p ./kafka-producer
CMD ["sh", "-c", "java -jar ./kafka-producer/build/libs/kafka-producer.jar --topic $TOPIC --num-records $NUM_RECORDS --producer-props bootstrap.servers=$SERVER_IP:$SERVER_PORT --throughput $THROUGHPUT --record-size $RECORD_SIZE"]