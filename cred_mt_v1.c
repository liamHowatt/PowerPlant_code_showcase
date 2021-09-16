#include <stdio.h>
#include <stdlib.h>
#include <string.h>
// #include <signal.h>
#include <pthread.h>
#include "MQTTClient.h"
#include "cJSON.h"
#include <modbus.h>
// #include <time.h>

typedef enum dtype {BOOL, INT, REAL} dtype;
typedef struct Tag {dtype type; int addr;} Tag;
typedef struct ModbusThreadData {
    int is_initialized;
    modbus_t *conn;
    int indx;

    int addr;
    dtype type;

    int message_len;
    char message_out[50];
} ModbusThreadData;

void *modbus_thread(void *arg) {
    ModbusThreadData *data = (ModbusThreadData *) arg;

    uint8_t coil;
    uint16_t int_;
    uint16_t real[2];

    switch (data->type) {
        case BOOL:
            if (modbus_read_bits(data->conn, data->addr, 1, &coil) == -1) {
                puts("modbus BOOL read failed");
                exit(0);
            }
            if (coil == TRUE) {
                strcpy(data->message_out, "[true,false,false,false,false,false,false,false]");
                data->message_len = 48;
            } else {
                strcpy(data->message_out, "[false,false,false,false,false,false,false,false]");
                data->message_len = 49;
            }
            break;
        case INT:
            if (modbus_read_registers(data->conn, data->addr, 1, &int_) == -1) {
                puts("modbus INT read failed");
                exit(0);
            }
            data->message_len = sprintf(data->message_out, "[%u]", int_);
            break;
        case REAL:
            if (modbus_read_registers(data->conn, data->addr, 2, real) == -1) {
                puts("modbus REAL read failed");
                exit(0);
            }
            data->message_len = sprintf(data->message_out, "[%u,%u]", real[0], real[1]);
            break;
    }

    data->is_initialized = 1;
    return NULL;
}


int main() {

    MQTTClient mqtt_client;
    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
    MQTTClient_create(
        &mqtt_client,
        "tcp://localhost:1883",
        "JSONGetter",
        MQTTCLIENT_PERSISTENCE_NONE,
        NULL
    );
    conn_opts.keepAliveInterval = 20;
    conn_opts.cleansession = 1;
    puts("connecting mqtt...");
    if(MQTTClient_connect(mqtt_client, &conn_opts) != MQTTCLIENT_SUCCESS){
        puts("MQTT connect failed");
        exit(0);
    }
    puts("subscribing...");
    char TAG_TABLE_TOPIC[] = "nodered/plc/tag_table/1";
    if (MQTTClient_subscribe(mqtt_client, TAG_TABLE_TOPIC, 0) != MQTTCLIENT_SUCCESS) {
        puts("MQTT subscribe fail");
        exit(0);
    }

    char *topic;
    int topic_len;
    MQTTClient_message *message;
    puts("receiving tag table...");
    if (MQTTClient_receive(mqtt_client, &topic, &topic_len, &message, 0) != MQTTCLIENT_SUCCESS){
        puts("MQTT recevie fail");
        exit(0);
    }
    if(!message){
        puts("MQTT no retained tag table to read");
        exit(0);
    }
    MQTTClient_free(topic);
    // printf("topic: %s\n", topic);
    // printf("topic length: %d\n", topic_len);
    // printf("message:\n%s\n", (char *)message->payload);
    puts("unsubscribing...");
    if(MQTTClient_unsubscribe(mqtt_client, TAG_TABLE_TOPIC) != MQTTCLIENT_SUCCESS) {
        puts("MQTT unsubscribe failed");
        exit(0);
    }

    puts("parsing...");
    cJSON *tag_table = cJSON_Parse((char *)message->payload);
    MQTTClient_freeMessage(&message);
    // printf("seeking...\n");
    // int sz = cJSON_GetArraySize(tag_table);
    // cJSON *tag = cJSON_GetArrayItem(tag_table, sz - 2);
    // cJSON *tag_name = cJSON_GetObjectItemCaseSensitive(tag, "tag_name");
    // printf("%s\n", tag_name->valuestring);
    // cJSON *fc = cJSON_GetObjectItemCaseSensitive(tag, "fc");
    // cJSON *fc_read = cJSON_GetObjectItemCaseSensitive(fc, "read");
    // printf("%d\n", fc_read->valueint);

    int sz = cJSON_GetArraySize(tag_table);
    if (sz == 0) {
        puts("tag table empty");
        exit(0);
    }
    char **tag_names = malloc(sz * sizeof(char *));
    int longest = 0;
    Tag *tags = malloc(sz * sizeof(Tag));
    cJSON *tag = tag_table->child;
    int i = 0;
    while(1) {
        cJSON *tag_name = cJSON_GetObjectItemCaseSensitive(tag, "tag_name");
        tag_names[i] = tag_name->valuestring;
        int tag_name_len = strlen(tag_name->valuestring);
        if (tag_name_len > longest) {
            longest = tag_name_len;
        }
        cJSON *tag_address = cJSON_GetObjectItemCaseSensitive(tag, "address");
        tags[i].addr = tag_address->valueint;
        cJSON *tag_fc = cJSON_GetObjectItemCaseSensitive(tag, "fc");
        cJSON *tag_fc_write = cJSON_GetObjectItemCaseSensitive(tag_fc, "write");
        // {"b":5, "i":6, "r":16}
        switch (tag_fc_write->valueint) {
            case 5:
                tags[i].type = BOOL;
                break;
            case 6:
                tags[i].type = INT;
                break;
            case 16:
                tags[i].type = REAL;
                break;
        }
        if(!tag->next){
            break;
        }
        tag = tag->next;
        i++;
    }

    puts("connecting to modbus...");
    modbus_t *modbus;
    modbus = modbus_new_tcp("192.168.2.125", 502);
    if (modbus_connect(modbus) == -1) {
        puts("modbus connect failed");
        exit(0);
    }

    ModbusThreadData thread_data;
    thread_data.is_initialized = 0;
    thread_data.conn = modbus;
    pthread_t thread_handle;

    char PUB_TOPIC[] = "nodered/plc/announce/1/normal/";
    int PUB_TOPIC_LEN = strlen(PUB_TOPIC);
    char *topic_out = malloc((PUB_TOPIC_LEN + longest) * sizeof(char));
    strcpy(topic_out, PUB_TOPIC);
    puts("entering loop...");
    for (int j=0; j<100; j++) {
        // printf("%d\n", j);
        for (int i=0; i<sz; i++) {
            
            thread_data.addr = tags[i].addr;
            thread_data.type = tags[i].type;
            int is_initialized = thread_data.is_initialized;
            int indx;
            int message_len;
            char message_out[50];
            if (thread_data.is_initialized == 1) {
                indx = thread_data.indx;
                message_len = thread_data.message_len;
                strcpy(message_out, thread_data.message_out);
            }
            thread_data.indx = i;

            pthread_create(&thread_handle, NULL, modbus_thread, &thread_data);
            
            if (thread_data.is_initialized == 1) {
                char *name = tag_names[indx];
                strcpy(topic_out + PUB_TOPIC_LEN, name);
                if (MQTTClient_publish(
                    mqtt_client,
                    topic_out,
                    message_len,
                    message_out,
                    0,
                    0,
                    NULL
                ) != MQTTCLIENT_SUCCESS) {
                    puts("publish failed");
                    exit(0);
                }
            }

            pthread_join(thread_handle, NULL);
        }
    }


    puts("disconnecting modbus...");
    modbus_close(modbus);
    modbus_free(modbus);

    puts("freeing tag table...");
    cJSON_Delete(tag_table);
    puts("freeing others...");
    free(tag_names);
    free(tags);
    free(topic_out);

    puts("disconnecting mqtt...");
    MQTTClient_disconnect(mqtt_client, 1000);
    MQTTClient_destroy(&mqtt_client);

    return 0;
}