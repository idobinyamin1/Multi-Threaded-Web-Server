#include "segel.h"
#include "request.h"
#include <math.h>

//
// server.c: A very, very simple web server
//
// To run:
//  ./server <portnum (above 2000)>
//
// Repeatedly handles HTTP requests sent to this port number.
// Most of the work is done within routines written in request.c
//

// HW3: Parse the new arguments too

int port;
int pool_size;
int max_queue_size;
char schedalg[10] = {0};

pthread_mutex_t mutex;
pthread_cond_t cond;
pthread_mutex_t mutex2;
pthread_cond_t cond2;
//Queue functions

struct node
{

    struct job_data data;
    struct node *p;
    struct node *n;
};
typedef struct node *Node;

struct queue
{

    int data;
    int size;
    struct node *first;
    struct node *last;
};
typedef struct queue *Queue;

Queue createQueue()
{
    Queue queue = (Queue)malloc(sizeof(struct queue));
    queue->first = (Node)malloc(sizeof(struct node));
    queue->last = (Node)malloc(sizeof(struct node));
    queue->first->n = queue->last;
    queue->first->p = NULL;
    queue->last->p = queue->first;
    queue->last->n = NULL;
   queue->size = 0;
    queue->data=0;
    return queue;
}

Node enqueue(Queue q, job_Data data)
{
    Node node = (Node)malloc(sizeof(struct node));
    
    node->data = data;
   
    node->p = q->first;
    node->n = q->first->n;
    node->n->p = node;
    q->first->n = node;
    q->size++;
    return node;
}
void remove_by_node(Queue q, Node node)
{
    if (node->p != NULL)
    {
        node->p->n = node->n;
    }
    else
    {
        q->first = node->n;
    }
    if (node->n != NULL)
    {
        node->n->p = node->p;
    }
    else
    {
        q->last = node->p;
    }

    free(node);
    q->size--;
    return;
}
int removebyindex(Queue q, int index)
{
    if (q->size == 0)
    {
        return -1;
    }
    Node temp = q->first->n;
    while (index != 0)
    {
        temp = temp->n;
        index--;
    }
    int fd = temp->data.fd;
    temp->p->n = temp->n;
    temp->n->p = temp->p;
    q->size--;
    free(temp);
    return fd;
}
job_Data dequeue(Queue q)
{
    job_Data fake;
    
    fake.fd = -2;
    if (q->first->n == q->last)
    {
        return fake;
    }
    Node to_delete = q->last->p;
   
    to_delete->p->n = q->last;
    q->last->p = to_delete->p;
    job_Data data = to_delete->data;
    free(to_delete);
    q->size--;
    return data;
}

void destroyQueue(Queue q)
{
    if (q->first->n == q->last)
    {
        free(q->first);
        free(q->last);
        free(q);
        return;
    }
    Node temp = q->first->n;
    while (temp != q->last)
    {
        Node to_delete = temp;
        temp = temp->n;
        free(to_delete);
    }
    free(q->first);
    free(q->last);
    free(q);
}





void getargs(int argc, char *argv[])
{
    if (argc < 2)
    {
        fprintf(stderr, "Usage: %s <port>\n", argv[0]);
        exit(1);
    }
    port = atoi(argv[1]);
    if (argc > 2)
    {
        pool_size = atoi(argv[2]);
        max_queue_size = atoi(argv[3]);
    }
}
void *run_thread(Queue *q)
{
       thread_Stats stats=(thread_Stats)malloc(sizeof(struct thread_stats));
       stats->thread_dynamic=0;
       stats->thread_count=0;
       stats->thread_static=0;
    struct timeval pickup_time;
       pthread_mutex_lock(&mutex);
        
        
         stats->thread_id=q[2]->data++;

         pthread_mutex_unlock(&mutex);

    
    while (1)
    {
        pthread_mutex_lock(&mutex);

        while (q[0]->size == 0)
        {
            pthread_cond_wait(&cond, &mutex);
        }
        job_Data temp = dequeue(q[0]);
        pthread_mutex_unlock(&mutex);
        gettimeofday(&pickup_time, NULL);
       
        timersub(&pickup_time,&temp.req_arrival,&temp.req_dispatch); 
        
        pthread_mutex_lock(&mutex);
        Node temp_added = enqueue(q[1], temp);
        pthread_mutex_unlock(&mutex);
           stats->thread_count++;
        requestHandle(temp.fd,stats,temp);

        pthread_mutex_lock(&mutex);

        remove_by_node(q[1], temp_added);

        pthread_mutex_unlock(&mutex);
        Close(temp.fd);
        pthread_cond_signal(&cond2);
    }
}

//args: [portnum] [threads] [queue_size] [schedalg]
int main(int argc, char *argv[])
{
    int listenfd, connfd, clientlen;
    struct sockaddr_in clientaddr;
    struct timeval arrive_time;
    srand(time(0));
    
    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&cond, NULL);
    pthread_mutex_init(&mutex2, NULL);
    pthread_cond_init(&cond2, NULL);
    getargs(argc, argv);

    createQueue();

    Queue q[3]; // 0 - waiting 1 - executing
    q[0] = createQueue();
    q[1] = createQueue();
     
     q[2]=createQueue();
    q[2]->data=0;
    int max_req=atoi(argv[3]);
    //
    // HW3: Create some threads...
    //

    pthread_t *pool = (pthread_t *)malloc(sizeof(pthread_t) * pool_size);
    for (int i = 0; i < pool_size; i++)
    {
        if (pthread_create(&pool[i], NULL, (void *)run_thread, q))
        {
        }
    }

    listenfd = Open_listenfd(port);
    while (1)
    {
        clientlen = sizeof(clientaddr);
        connfd = Accept(listenfd, (SA *)&clientaddr, (socklen_t *)&clientlen);
        gettimeofday(&arrive_time, NULL);
        
        //
        // HW3: In general, don't handle the request in the main thread.
        // Save the relevant info in a buffer and have one of the worker threads
        // do the work.
        //
        if (q[1]->size + q[0]->size >= max_req)
        {
            if (strcmp(argv[4], "block") == 0)
            {

                pthread_mutex_lock(&mutex2);
                while (q[1]->size + q[0]->size >= max_req)
                {
                    pthread_cond_wait(&cond2, &mutex2);
                }
                pthread_mutex_unlock(&mutex2);
            }

            else if (strcmp(argv[4], "random") == 0)

            {
                pthread_mutex_lock(&mutex);
                if (q[1]->size + q[0]->size >= max_req)
                {
                    int num_of_drops = (q[0]->size * 0.5);
                    num_of_drops = q[0]->size -num_of_drops; //round up
                    
                    

                    if (num_of_drops == 0)
                    {
                        Close(connfd);
                        pthread_mutex_unlock(&mutex);

                        continue;
                    }
                    for (int i = 0; i < num_of_drops; i++)
                    {
                        int to_drop_idx = rand() % q[0]->size;
                        int to_drop_fd = removebyindex(q[0], to_drop_idx);

                        Close(to_drop_fd);
                    }
                }
                pthread_mutex_unlock(&mutex);
            }
            else if (strcmp(argv[4], "dh") == 0)
            {
                pthread_mutex_lock(&mutex);
                if (q[1]->size + q[0]->size >= max_req)
                {

                    job_Data to_drop_data = dequeue(q[0]);
                    pthread_mutex_unlock(&mutex);

                    Close(to_drop_data.fd);
                }
                else{
                pthread_mutex_unlock(&mutex);
                }
            }

            else if (strcmp(argv[4], "dt") == 0)
            {
                pthread_mutex_lock(&mutex);
                if (q[1]->size + q[0]->size >= max_req)
                {

                    pthread_mutex_unlock(&mutex);
                    Close(connfd);
                    continue;
                }
                pthread_mutex_unlock(&mutex);
            }
        }
        job_Data t_data;
        t_data.fd = connfd;
        t_data.req_arrival=arrive_time;
        pthread_mutex_lock(&mutex);
        enqueue(q[0], t_data);

        pthread_mutex_unlock(&mutex);
        pthread_cond_signal(&cond);
    }

    for (int i = 0; i < pool_size; i++)
    {
        if (pthread_join(pool[i], NULL) != 0)
        {
        }
    }
    destroyQueue(q[2]);
    destroyQueue(q[0]);
    destroyQueue(q[1]);
    
    free(pool);

    pthread_mutex_destroy(&mutex);
   
    pthread_cond_destroy(&cond);

    pthread_mutex_destroy(&mutex2);
   
    pthread_cond_destroy(&cond2);

}
