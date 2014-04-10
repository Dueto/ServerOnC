/* 
 * File:   main.c
 * Author: amatveev
 *
 * Created on April 8, 2014, 3:41 PM
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <websock/websock.h>
#include <mysql/mysql.h>

struct mysql_database
{
    MYSQL *database;
    char *server;
    char *user;
    char *password;
    char *database_name; 
};

//MYSQL_TYPE_DOUBLE

char *form_sql_statement(char *table_columns,
                         char *table_name,
                         char *begin_time,
                         char *end_time)
{    
    char *bufer;
    bufer = (char *) malloc (sizeof(char) * 1000);
    memset(bufer, 0, (sizeof(char) * 1000));
    
    bufer = strcat(bufer, "SELECT time");
    bufer = strcat(bufer, table_columns);
    bufer = strcat(bufer, " FROM ");
    bufer = strcat(bufer, table_name);        
    bufer = strcat(bufer, " WHERE time>=FROM_UNIXTIME(");
    bufer = strcat(bufer, begin_time);
    bufer = strcat(bufer, ")");
    bufer = strcat(bufer, " AND time<=FROM_UNIXTIME(");
    bufer = strcat(bufer, end_time);
    bufer = strcat(bufer, ")");
    
//    bufer = strcat(bufer, "AND * LIKE '%");
//    bufer = strcat(bufer, aggregation);
//    bufer = strcat(bufer, "%'");
    
    return bufer;
};


MYSQL_RES *get_result(char *sql_statement, struct mysql_database *mysql)
{     
  MYSQL_RES *res;
  if (mysql_real_query(mysql->database , sql_statement, strlen(sql_statement))) 
  {  
      fprintf(stderr, "%s\n", mysql_error(mysql->database));      
      return NULL;
  }     
  res = mysql_use_result(mysql->database);    
  return res;    
};

struct mysql_database *mysql_database_init()
{   
  MYSQL *databasep;  
  struct mysql_database *mysql;
  databasep = mysql_init(NULL);
  mysql->database =  databasep;
  mysql->server = "localhost";
  mysql->user = "root";
  mysql->password = "root";
  mysql->database_name = "adei";
  
  if(!mysql_real_connect(mysql->database, mysql->server,
         mysql->user, mysql->password, mysql->database_name, 0, NULL, 0)) 
  {
      fprintf(stderr, "%s\n", mysql_error(mysql->database));
      exit(1);
  }
  
  return mysql;
};

MYSQL_RES *get_column_names(char *table_name, char *aggregation, struct mysql_database *mysql)
{
    MYSQL_RES *res;      
    char *bufer;
    bufer = (char *) malloc (sizeof(char) * 200);    
    memset(bufer, 0, (sizeof(char) * 200));
    
    bufer = strcat(bufer, "SHOW COLUMNS FROM ");
    bufer = strcat(bufer, table_name);
    bufer = strcat(bufer, " WHERE field like '%");
    bufer = strcat(bufer, aggregation);
    bufer = strcat(bufer, "%' OR field = 'ns'");
    
    res = get_result(bufer, mysql);    
    if(res == NULL)
    {   
        return NULL;
    }    
    return res;

};

char *form_table_columns(MYSQL_RES *columns)
{
   MYSQL_ROW row;   
   char *bufer;
   bufer = (char *) malloc (sizeof(char) * 1000);
   memset(bufer, 0, (sizeof(char) * 1000));
   
   while ((row = mysql_fetch_row(columns)) != NULL)
   {
      bufer = strcat(bufer, ",");
      bufer = strcat(bufer, row[0]);      
   }  
   return bufer;
};

char *get_representation(const double number) 
{
    char *representation = (char *) malloc(sizeof(double));
    memset(representation, 0, sizeof(double));
    memcpy(representation, &number, sizeof(double));    
    return representation;
};

int concatenate(char* string, int string_length, char* to_concatenat, int concatanate_length)
{
    memcpy(&string[string_length], to_concatenat, sizeof(char) * concatanate_length);
    return string_length + (sizeof(char) * concatanate_length);
}

int 
onmessage(libwebsock_client_state *state, libwebsock_message *msg)
{   
  fprintf(stderr, "Received message from client: %d\n", state->sockfd);
  fprintf(stderr, "Message opcode: %d\n", msg->opcode);
  fprintf(stderr, "Payload Length: %llu\n", msg->payload_len);
  fprintf(stderr, "Initial message: %s\n", msg->payload);
  
  char *copy = (char *) malloc (strlen(msg->payload));  
  memset(copy, 0, (strlen(msg->payload)));
  
  strncpy(copy, msg->payload, strlen(msg->payload));
  
  char* table_name = strsep(&copy, ";"); 
  char* begin_time = strsep(&copy, ";");   
  char* end_time = strsep(&copy, ";");  
  char* is_one_portion = strsep(&copy, ";");  
  char* aggregation = strsep(&copy, ";");
  char* channel_count_string = strsep(&copy, ";");
  int channel_count = (int) strtol(channel_count_string, (char **)NULL, 10);
  
  fprintf(stderr, "Table Name: %s\n", table_name);
  fprintf(stderr, "Begin time: %s\n", begin_time);
  fprintf(stderr, "End time: %s\n", end_time);
  fprintf(stderr, "Is one portion: %s\n", is_one_portion);
  fprintf(stderr, "Aggregation: %s\n", aggregation);
  fprintf(stderr, "Channel count: %i\n", channel_count);
  
    
  
  struct mysql_database *mysql = mysql_database_init();  
  MYSQL_RES *columns = get_column_names(table_name, aggregation, mysql); 
  if(columns == NULL)
  {
      libwebsock_send_text(state, "No aggregation mode in table.");      
  }
  else  
  {      
      char *table_columns = form_table_columns(columns);                   
      char *sql_statement = form_sql_statement(table_columns, table_name, begin_time, end_time); 
      MYSQL_ROW row;        
      MYSQL_RES *res = get_result(sql_statement, mysql);
      if(res == NULL)
      {
          libwebsock_send_text(state, "No data in request.");      
      }
      else
      {   
          char *bufer;          
          unsigned long long sum = 0;           
          while((row = mysql_fetch_row(res)) != NULL)
          {   
              if(sum == 0)
              {
                  bufer = (char *) malloc (strlen(row[0])); 
                  memset(bufer, 0, strlen(row[0]));   
                  
                  sum = sum + strlen(row[0]);
                  bufer = strncpy(bufer, row[0], strlen(row[0]));                  
                  //sum = Concatenate(bufer, sum, row[0], strlen(row[0]));
                 
              }
              else
              {
                  sum = sum + strlen(row[0]);
                  bufer = (char *) realloc (bufer, sum); 
                  
                  sum = concatenate(bufer, sum - strlen(row[0]), row[0], strlen(row[0]));                  
              }  
              int i = 0;
              for(i = 0; i < channel_count; i++)
              { 
                  double point = strtod(row[i + 1], (char **)NULL); 
                  char *string_point = get_representation(point);
                  
                  sum = sum + sizeof(double) + 1;                   
                  bufer = (char *) realloc (bufer, sum); 
                  
                  sum = concatenate(bufer, sum - (sizeof(double) + 1), string_point, (sizeof(double) + 1));   
              }
          }       fprintf(stderr, "VOROBEI\n");
          libwebsock_send_binary(state, bufer, sum);            
          mysql_free_result(res);  
          mysql_close(mysql->database);
         // free(bufer); 
          //free(table_columns);
         // free(sql_statement); 
         //free(copy);
          
          
          
      }       
  }  
  return 0;
}

int
onopen(libwebsock_client_state *state)
{
  fprintf(stderr, "Client opened connection: %d\n", state->sockfd);
  return 0;
}

int
onclose(libwebsock_client_state *state)
{
  fprintf(stderr, "Client closed connection: %d\n", state->sockfd);
  return 0;
}

int
main(int argc, char *argv[])
{  
  my_init();
  libwebsock_context *ctx = NULL;
   
  ctx = libwebsock_init();
  if(ctx == NULL) 
  {
    fprintf(stderr, "Error during libwebsock_init.\n");
    exit(1);
  }
  
  libwebsock_bind(ctx, "localhost" , "12345");
  fprintf(stderr, "libwebsock listening on port %s\n", "12345");
  
  ctx->onmessage = onmessage;
  ctx->onopen = onopen;
  ctx->onclose = onclose;
  
  libwebsock_wait(ctx);
  
  fprintf(stderr, "Exiting.\n");
  return 0;
}