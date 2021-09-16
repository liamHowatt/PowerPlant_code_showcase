#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "MQTTClient.h"
#include "cJSON.h"
#include <modbus.h>

typedef enum dtype {BOOL, INT, REAL} dtype;
typedef struct Tag {dtype type; int addr;} Tag;

void *mylloc(size_t size) {
    void *ptr = malloc(size);
    if (!ptr) {
        puts("memory could not be allocated");
        exit(1);
    }
    return ptr;
}

MQTTClient make_mqtt_client() {
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
    // connect to MQTT broker
    if(MQTTClient_connect(mqtt_client, &conn_opts) != MQTTCLIENT_SUCCESS){
        puts("MQTT connect failed");
        exit(0);
    }
    return mqtt_client;
}

int main() {

    /////////////////////////////////
    // Connect to MQTT
    puts("connecting mqtt...");
    MQTTClient mqtt_client = make_mqtt_client();
    ////////////////////////////////

    ////////////////////////////////
    // Get tag table from retained MQTT topic
    puts("getting tag table from mqtt...");
    MQTTClient_message *tag_table_message;
    {
        char TAG_TABLE_TOPIC[] = "nodered/plc/tag_table/1";
        if (MQTTClient_subscribe(mqtt_client, TAG_TABLE_TOPIC, 0) != MQTTCLIENT_SUCCESS) {
            puts("MQTT subscribe fail");
            exit(0);
        }
        {
            char *topic;
            int topic_len;
            if (MQTTClient_receive(mqtt_client, &topic, &topic_len, &tag_table_message, 0) != MQTTCLIENT_SUCCESS){
                puts("MQTT recevie fail");
                exit(0);
            }
            MQTTClient_free(topic);
        }
        if(!tag_table_message){
            puts("MQTT no retained tag table to read");
            exit(0);
        }
        if(MQTTClient_unsubscribe(mqtt_client, TAG_TABLE_TOPIC) != MQTTCLIENT_SUCCESS) {
            puts("MQTT unsubscribe failed");
            exit(0);
        }
    }
    ////////////////////////////////

    
    puts("parsing...");
    cJSON *tag_table = cJSON_Parse((char *)tag_table_message->payload);
    MQTTClient_freeMessage(&tag_table_message);

    int sz = cJSON_GetArraySize(tag_table);
    if (sz == 0) {
        puts("tag table empty");
        exit(0);
    }
    char **tag_names = mylloc(sz * sizeof(char *));
    int longest = 0;
    int max_bool_reg = -1;
    int max_hold_reg = -1;
    Tag *tags = mylloc(sz * sizeof(Tag));
    {
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
                    if (tag_address->valueint > max_bool_reg) {
                        max_bool_reg = tag_address->valueint;
                    }
                    break;
                case 6:
                    tags[i].type = INT;
                    if (tag_address->valueint > max_hold_reg) {
                        max_hold_reg = tag_address->valueint;
                    }
                    break;
                case 16:
                    tags[i].type = REAL;
                    if (tag_address->valueint > max_hold_reg) {
                        max_hold_reg = tag_address->valueint + 1;
                    }
                    break;
            }
            if(!tag->next){
                break;
            }
            tag = tag->next;
            i++;
        }
    }

    puts("connecting to modbus...");
    modbus_t *modbus;
    modbus = modbus_new_tcp("192.168.2.125", 502);
    if (modbus_connect(modbus) == -1) {
        puts("modbus connect failed");
        exit(0);
    }

    puts("preparing to enter loop...");
    char message_out[51] = "";
    char PUB_TOPIC[] = "nodered/plc/announce/1/normal/";
    int PUB_TOPIC_LEN = strlen(PUB_TOPIC);
    char *topic_out = mylloc((PUB_TOPIC_LEN + longest + 1) * sizeof(char));
    strcpy(topic_out, PUB_TOPIC);
    uint8_t *bool_reg;
    if (max_bool_reg >= 0) {
        bool_reg = mylloc((max_bool_reg + 1) * sizeof(uint8_t));
    }
    uint16_t *hold_reg;
    if (max_hold_reg >= 0) {
        hold_reg = mylloc((max_hold_reg + 1) * sizeof(uint16_t));
    }
    puts("entering loop...");
    for (int j=0; j<1000; j++) {
        if (max_bool_reg >= 0) {
            if (modbus_read_bits(modbus, 0, max_bool_reg + 1, bool_reg) == -1) {
                puts("modbus coil register read failed");
                exit(0);
            }
        }
        if (max_bool_reg >= 0) {
            if (modbus_read_registers(modbus, 0, max_hold_reg + 1, hold_reg) == -1) {
                puts("modbus holding register read failed");
                exit(0);
            }
        }
        for (int i=0; i<sz; i++) {
            int message_len;
            int addr = tags[i].addr;
            switch (tags[i].type) {
                case BOOL:
                    if (bool_reg[addr] == TRUE) {
                        strcpy(message_out, "[true,false,false,false,false,false,false,false]");
                        message_len = 48;
                    } else {
                        strcpy(message_out, "[false,false,false,false,false,false,false,false]");
                        message_len = 49;
                    }
                    break;
                case INT:
                    message_len = sprintf(message_out, "[%u]", hold_reg[addr]);
                    break;
                case REAL:
                    message_len = sprintf(message_out, "[%u,%u]", hold_reg[addr], hold_reg[addr + 1]);
                    break;
            }

            strcpy(topic_out + PUB_TOPIC_LEN, tag_names[i]);
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
    }

    puts("disconnecting and freeing...");
    if (max_bool_reg >= 0) {
        free(bool_reg);
    }
    if (max_hold_reg >= 0) {
        free(hold_reg);
    }
    modbus_close(modbus);
    modbus_free(modbus);
    free(tag_names);
    free(tags);
    free(topic_out);
    cJSON_Delete(tag_table);
    MQTTClient_disconnect(mqtt_client, 1000);
    MQTTClient_destroy(&mqtt_client);

    return 0;
}