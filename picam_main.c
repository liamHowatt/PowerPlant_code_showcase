#include <pthread.h>
#include "MQTTClient.h"
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>

// gcc picam_main.c -o picam_main -O2 -Wall -lpthread -lpaho-mqtt3c
// ./picam_main "tcp://192.168.2.19:1883" 1 1

char *mqtt_uri;
char ping_topic[19];
char take_pic_topic[23];
char reply_topic_suff[25];
char raspistill_cmd[150];

int print_code(int code, char *cmd){
	if(code != 0){
		printf("%s: err code %d\n", cmd, code);
		sleep(1);
	}
	return code;
}

void my_mqtt_init(MQTTClient *mqtt_hndl){
	while(1){

		if(print_code(MQTTClient_create(
			mqtt_hndl,
			mqtt_uri,
			"",
			MQTTCLIENT_PERSISTENCE_NONE,
			NULL
		), "create client") != 0) continue;

		MQTTClient_connectOptions con_opts = MQTTClient_connectOptions_initializer;
		con_opts.keepAliveInterval = 5;
		con_opts.cleansession = 1;
		con_opts.reliable = 1;
		con_opts.connectTimeout = 5;
		con_opts.retryInterval = 0;
		if(print_code(MQTTClient_connect(
			*mqtt_hndl,
			&con_opts
		), "connect") != 0) continue;

		break;
	}
}

void *pinger_thread_func(void *arg){

	while(1){
		MQTTClient mqtt_hndl;
		my_mqtt_init(&mqtt_hndl);

		char ip[18];
		FILE *hostname = popen("hostname -I", "r");
		fgets(ip, 16, hostname);
		pclose(hostname);
		char *nl = strchr(ip, '\n');
		if(nl){
			*nl = '\0';
		}
		int ip_len = strlen(ip);

		while(1){
			if(print_code(MQTTClient_publish(
				mqtt_hndl,
				ping_topic,
				ip_len,
				ip,
				0,
				0,
				NULL
			), "publish ping") != 0) break;
			sleep(1);
		}
		
		MQTTClient_destroy(&mqtt_hndl);
	}

	// unreachable
	return NULL;
}

int main(int argc, char **argv){

	if(argc != 5){
		puts("usage: picam_main <mqtt broker URI> <cam group ID> <cam ID> <raspistill flags>");
		return 1;
	}
	mqtt_uri = argv[1];
	sprintf(ping_topic, "picam/%s/%s/ping", argv[2], argv[3]);
	sprintf(take_pic_topic, "picam/%s/all/take-pic", argv[2]);
	sprintf(reply_topic_suff, "picam/%s/%s/pic-reply/", argv[2], argv[3]);
	sprintf(raspistill_cmd, "raspistill -o - -k -n -e jpg -w 640 -h 480 %s > /home/pi/camera/image_pipe", argv[4]);

	pthread_t pinger_thread_handle;

	pthread_create(&pinger_thread_handle, NULL, pinger_thread_func, NULL);

	system("pkill raspistill > /dev/null");
	FILE *raspistill = popen(raspistill_cmd, "w");
	int img_pipe = open("/home/pi/camera/image_pipe", O_RDONLY);
	static char _buff[(10 << 20) + 2];
	_buff[0] = '\0';
	_buff[1] = '\0';
	char *buff = _buff + 2;

	while(1){

		MQTTClient mqtt_hndl;
		my_mqtt_init(&mqtt_hndl);

		while(1){
			if(print_code(MQTTClient_subscribe(
				mqtt_hndl,
				take_pic_topic,
				0
			), "subscribe to trigger") != 0) break;

			while(1){

				char *recv_topic;
				int recv_topicLen;
				MQTTClient_message *recv_message;
				int recv_failed = 0;
				while(1){
					if(print_code(MQTTClient_receive(
						mqtt_hndl,
						&recv_topic,
						&recv_topicLen,
						&recv_message,
						1000
					), "receive trigger") != 0){
						recv_failed = 1;
						break;
					}
					if(recv_message) break;
				}
				if(recv_failed) break;

				fputc('\n', raspistill);
				fflush(raspistill);
				char *p = buff;
				int num;
				int tot = 0;
				while(!( *(p - 2) == 0xff && *(p - 1) == 0xd9 )){
					num = read(img_pipe, p, 1<<20);
					p += num;
					tot += num;
				}
				printf("%d\n", tot);

				MQTTClient_free(recv_topic);
				char reply_topic[61];
				strcpy(reply_topic, reply_topic_suff);
				strncat(reply_topic, (char *)recv_message->payload, 36);
				MQTTClient_freeMessage(&recv_message);

				if(print_code(MQTTClient_publish(
					mqtt_hndl,
					reply_topic,
					tot,
					buff,
					0,
					0,
					NULL
				), "reply pic") != 0) break;

			}

			break;
		}

		MQTTClient_destroy(&mqtt_hndl);
	}

	// unreachable
	close(img_pipe);
	pclose(raspistill);

	return 0;
}
