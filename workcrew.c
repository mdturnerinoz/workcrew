#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>
#include "control.h"
#include "queue.h"
#include "dbug.h"

/* 
  This file and related *.c and *.h files are part of an excellent series
  of articles originally created by Daniel Robbins via the IBM Developer
  Works website ("Common threads: POSIX threads explained, Part 3" found
  at http://www.ibm.com/developerworks/library/l-posix3/). (While the other
  two parts of the series are equally as instructive, the following code
  involving pthread "condition variables" is probably some of the least
  understood code my most pthread developers.)
 
  The following commentary has been extraced verbatim from the original
  article text (for clarity-sake and so I remember the salient details
  as well.
 
  N.B. While all of the commentary exists in this module, it also specifically
       addresses code in the other files as well. See the README.txt file
       associated with this probject for more details.
 
  Condition variables explained

  I ended my previous article by describing a particular dilemma: how does a
  thread deal with a situation where it is waiting for a specific condition
  to become true? It could repeatedly lock and unlock a mutex, each time
  checking a shared data structure for a certain value. But this is a waste
  of time and resources, and this form of busy polling is extremely inefficient.
  The best way to do this is to use the pthread_cond_wait() call to wait on a
  particular condition to become true.
  
  It's important to understand what pthread_cond_wait() does -- it's the heart
  of the POSIX threads signalling system, and also the hardest part to
  understand.
  
  First, let's consider a scenario where a thread has locked a mutex, in order
  to take a look at a linked list, and the list happens to be empty. This
  particular thread can't do anything -- it's designed to remove a node from
  the list, and there are no nodes available. So, this is what it does:
  
  While still holding the mutex lock, our thread will call
  pthread_cond_wait(&mycond,&mymutex). The pthread_cond_wait() call is rather
  complex, so we'll step through each of its operations one at a time.
  
  The first thing pthread_cond_wait() does is simultaneously unlock the mutex
  mymutex (so that other threads can modify the linked list) and wait on the
  condition mycond (so that pthread_cond_wait() will wake up when it is
  "signalled" by another thread). Now that the mutex is unlocked, other threads
  can access and modify the linked list, possibly adding items.
  
  At this point, the pthread_cond_wait() call has not yet returned. Unlocking
  the mutex happens immediately, but waiting on the condition mycond is normally
  a blocking operation, meaning that our thread will go to sleep, consuming no
  CPU cycles until it is woken up. This is exactly what we want to happen. Our
  thread is sleeping, waiting for a particular condition to become true,
  without performing any kind of busy polling that would waste CPU time. From
  our thread's perspective, it's simply waiting for the pthread_cond_wait()
  call to return.
  
  Now, to continue the explanation, let's say that another thread (call it
  "thread 2") locks mymutex and adds an item to our linked list. Immediately
  after unlocking the mutex, thread 2 calls the function
  pthread_cond_broadcast(&mycond). By doing so, thread 2 will cause all threads
  waiting on the mycond condition variable to immediately wake up. This means
  that our first thread (which is in the middle of a pthread_cond_wait() call)
  will now wake up.
  
  Now, let's take a look at what happens to our first thread. After thread 2
  called pthread_cond_broadcast(&mymutex) you might think that thread 1's
  pthread_cond_wait() will immediately return. Not so! Instead,
  pthread_cond_wait() will perform one last operation: relock mymutex. Once
  pthread_cond_wait() has the lock, it will then return and allow thread 1 to
  continue execution. At that point, it can immediately check the list for
  any interesting changes.  
 
  Code Walkthrough
 
  Now it's time for a quick code walkthrough. The first struct defined is
  called "wq", and contains a data_control and a queue header. The data_
  control structure will be used to arbitrate access to the entire queue,
  including the nodes in the queue. Our next job is to define the actual work
  nodes. To keep the code lean to fit in this article, all that's contained
  here is a job number.

  Next, we create the cleanup queue. The comments explain how this works.
  OK, now let's skip the threadfunc(), join_threads(), create_threads() and
  initialize_structs() calls, and jump down to main(). The first thing we do
  is initialize our structures -- this includes initializing our data_controls
  and queues, as well as activating our work queue.
 
  Cleanup Special
 
  Now it's time to initialize our threads. If you look at our create_threads()
  call, everything will look pretty normal -- except for one thing. Notice
  that we are allocating a cleanup node, and initializing its threadnum and
  TID components. We also pass a cleanup node to each new worker thread as
  an initial argument. Why do we do this?
 
  Because when a worker thread exits, it'll attach its cleanup node to the
  cleanup queue, and terminate. Then, our main thread will detect this addition
  to the cleanup queue (by use of a condition variable) and dequeue the node.
  Because the TID (thread id) is stored in the cleanup node, our main thread
  will know exactly which thread terminated. Then, our main thread will call
  pthread_join(tid), and join with the appropriate worker thread. If we didn't
  perform such bookkeeping, our main thread would need to join with worker
  threads in an arbitrary order, possibly in the order that they were created.
  Because the threads may not necessarily terminate in this order, our main
  thread could be waiting to join with one thread while it could have been
  joining with ten others. Can you see how this design decision can really
  speed up our shutdown code, especially if we were to use hundreds of worker
  threads?
 
  Creating Work
 
  Now that we've started our worker threads (and they're off performing their
  threadfunc(), which we'll get to in a bit), our main thread begins inserting
  items into the work queue. First, it locks wq's control mutex, and then
  allocates 16000 work packets, inserting them into the queue one-by-one.
  After this is done, pthread_cond_broadcast() is called, so that any sleeping
  threads are woken up and able to do the work. Then, our main thread sleeps
  for two seconds, and then deactivates the work queue, telling our worker
  threads to terminate. Then, our main thread calls the join_threads()
  function to clean up all the worker threads.
 
  threadfunc()
 
  Time to look at threadfunc(), the code that each worker thread executes.
  When a worker thread starts, it immediately locks the work queue mutex, gets
  one work node (if available) and processes it. If no work is available,
  pthread_cond_wait() is called. You'll notice that it's called in a very
  tight while() loop, and this is very important. When you wake up from a
  pthread_cond_wait() call, you should never assume that your condition is
  definitely true -- it will probably be true, but it may not. The while loop
  will cause pthread_cond_wait() to be called again if it so happens that the
  thread was mistakenly woken up and the list is empty.

  If there's a work node, we simply print out its job number, free it, and
  exit. Real code would do something more substantial. At the end of the
  while() loop, we lock the mutex so we can check the active variable as
  well as checking for new work nodes at the top of the loop. If you follow
  the code through, you'll find that if wq.control.active is 0, the while
  loop will be terminated and the cleanup code at the end of threadfunc()
  will begin.

  The worker thread's part of the cleanup code is pretty interesting. First,
  it unlocks the work_queue, since pthread_cond_wait() returns with the
  mutex locked. Then, it gets a lock on the cleanup queue, adds our cleanup
  node (containing our TID, which the main thread will use for its
  pthread_join() call), and then it unlocks the cleanup queue. After that,
  it signals any cq waiters (pthread_cond_signal(&cq.control.cond)) so
  that the main thread will know that there's a new node to process. We
  don't use pthread_cond_broadcast() because it's not necessary -- only one
  thread (the main thread) is waiting for new entries in the cleanup queue.
  Our worker thread prints a shutdown message, and then terminates, waiting
  to be pthread_joined() by the main thread when it calls join_threads().
 
  join_threads()
 
  If you want to see a simple example of how condition variables should be
  used, take a look at the join_threads() function. While we still have
  worker threads in existence, join_threads() loops, waiting for new cleanup
  nodes in our cleanup queue. If there is a new node, we dequeue the node,
  unlock the cleanup queue (so that other cleanup nodes can be added by
  our worker threads), join with our new thread (using the TID stored in
  the cleanup node), free the cleanup node, decrement the number of
  threads "out there", and continue. */

/* the work_queue holds tasks for the various threads to complete. */

struct work_queue
{
   data_control control;
   queue work;
} wq;


/* I added a job number to the work node.  Normally, the work node
   would contain additional data that needed to be processed. */

typedef struct work_node
{
   struct node *next;
   int jobnum;
} wnode;

/* the cleanup queue holds stopped threads.  Before a thread
   terminates, it adds itself to this list.  Since the main thread is
   waiting for changes in this list, it will then wake up and clean up
   the newly terminated thread. */

struct cleanup_queue
{
   data_control control;
   queue cleanup;
} cq;

/* I added a thread number (for debugging/instructional purposes) and
   a thread id to the cleanup node.  The cleanup node gets passed to
   the new thread on startup, and just before the thread stops, it
   attaches the cleanup node to the cleanup queue.  The main thread
   monitors the cleanup queue and is the one that performs the
   necessary cleanup. */

/* TBD: Marty, still need:
        1. commentary (including reference to original work)
        2. printf "workcrew" names
        3. debugdetail tested and msgs used or not
        4. pushed to github */

typedef struct cleanup_node
{
   struct node *next;
   int threadnum;
   pthread_t tid;
} cnode;

#define NUM_WORKERS    4   /* default values same as original code */
#define NUM_PACKETS 1600   /* default values same as original code */
#define SLEEP_SECS     2   /* default sleep time in seconds        */

int numpackets  = NUM_PACKETS;
int numworkers  = NUM_WORKERS;
int numthreads  = 0;
int debugdetail = 0;
int sleeptime   = SLEEP_SECS;

int getparams (int argc, char **argv )
{

   int status   = 0;
   char *cvalue = NULL;
   int index;
   int c;
   
   opterr = 0;
   
   while ((c = getopt (argc, argv, "w:p:s:d")) != -1)
   {
      switch (c)
      {
         case 'd':
            debugdetail = 1;
            break;
         case 'p':
            numpackets = atoi( optarg );
            break;
         case 's':
            sleeptime  = atoi( optarg );
            break;
         case 'w':
            numworkers = atoi( optarg );
            break;
         case '?':
            if (optopt == 'w' || optopt == 'p')
               fprintf (stderr, "Option -%c requires an argument.\n", optopt);
            else if (isprint (optopt))
               fprintf (stderr, "Unknown option `-%c'.\n", optopt);
            else
              fprintf (stderr, "Unknown option character `\\x%x'.\n", optopt);
           return status = 1;
         default:
           dabort();
      }
   }

   for (index = optind; index < argc; index++)
   {
      status = 1;
      printf ("Non-option argument %s\n", argv[index]);
   }

   printf("\nworkers(-w): %d, packets(-p): %d, sleeptime(-s): %d, debugdetail(-d): %s\n", numworkers, numpackets, 
                                                                sleeptime, debugdetail ? "on" : "off" );

   return status; 

}

void *threadfunc( void *myarg )
{

   wnode *mywork;
   cnode *mynode;

   mynode = ( cnode* )myarg;

   pthread_mutex_lock( &wq.control.mutex );

   while ( wq.control.active )
   {
      while ( wq.work.head == NULL && wq.control.active )
      {
         pthread_cond_wait( &wq.control.cond, &wq.control.mutex );
      }
      if (  ! wq.control.active )
         break;
      /* we got something! */
      mywork = ( wnode* )queue_get( &wq.work );
      pthread_mutex_unlock( &wq.control.mutex );
      /* perform processing... */
      if ( debugdetail )
      {
         printf( "Thread number %d processing job %d\n", mynode->threadnum, mywork->jobnum );
      }
      free( mywork );
      pthread_mutex_lock( &wq.control.mutex );
   }

   pthread_mutex_unlock( &wq.control.mutex );

   pthread_mutex_lock( &cq.control.mutex );
   queue_put( &cq.cleanup, ( node* )mynode );
   pthread_mutex_unlock( &cq.control.mutex );
   pthread_cond_signal( &cq.control.cond );
   if ( debugdetail )
   {
      printf( "thread %d shutting down...\n", mynode->threadnum );
   }
   return NULL;

}

void join_threads( void )
{
   cnode *curnode;

   while ( numthreads )
   {
      pthread_mutex_lock( &cq.control.mutex );

      /* below, we sleep until there really is a new cleanup node.  This
         takes care of any false wakeups... even if we break out of
         pthread_cond_wait(), we don't make any assumptions that the
         condition we were waiting for is true.  */

      while ( cq.cleanup.head == NULL )
      {
         pthread_cond_wait( &cq.control.cond, &cq.control.mutex );
      }

      /* at this point, we hold the mutex and there is an item in the
         list that we need to process.  First, we remove the node from
         the queue.  Then, we call pthread_join() on the tid stored in
         the node.  When pthread_join() returns, we have cleaned up
         after a thread.  Only then do we free() the node, decrement the
         number of additional threads we need to wait for and repeat the
         entire process, if necessary */

      curnode = ( cnode* )queue_get( &cq.cleanup );
      pthread_mutex_unlock( &cq.control.mutex );
      pthread_join( curnode->tid, NULL );
      if ( debugdetail )
      {
         printf( "joined with thread %d\n", curnode->threadnum );
      }
      free( curnode );
      numthreads--;
   }

   printf( "joined %d worker threads...\n", numworkers );

}


int create_threads( void )
{
   int x;
   cnode *curnode;

   for ( x = 0; x < numworkers; x++ )
   {
      curnode = malloc( sizeof( cnode ) );
      if (  ! curnode )
         return 1;
      curnode->threadnum = x;
      if ( pthread_create( &curnode->tid, NULL, threadfunc, ( void* )curnode ) )
         return 1;
      if ( debugdetail )
      {
         printf( "created thread %d\n", x );
      }
      numthreads++;
   }

   printf("created %d worker threads\n", numworkers );

   return 0;
}

void initialize_structs( void )
{
   numthreads = 0;
   if ( control_init( &wq.control ) )
      dabort();
   queue_init( &wq.work );
   if ( control_init( &cq.control ) )
   {
      control_destroy( &wq.control );
      dabort();
   }
   queue_init( &wq.work );
   control_activate( &wq.control );
}

void cleanup_structs( void )
{
   control_destroy( &cq.control );
   control_destroy( &wq.control );
}


int main( int argc, char **argv )
{

   int x;
   wnode *mywork;

   if ( getparams( argc, argv ) )
   {
      printf("parrms: -w num-workers - %d if not set\n", NUM_WORKERS );
      printf("        -p num-packets - %d if not set\n", NUM_PACKETS );
      printf("        -s sleep-secs  - %d if not set\n", SLEEP_SECS  );
      printf("        -d             - no debug detail if not set\n" );
      dabort();
   }

   initialize_structs();

   /* CREATION */

   if ( create_threads() )
   {
      printf( "Error starting threads... cleaning up.\n" );
      join_threads();
      dabort();
   }

   pthread_mutex_lock( &wq.control.mutex );
   for ( x = 0; x < numpackets; x++ )
   {
      mywork = malloc( sizeof( wnode ) );
      if (  ! mywork )
      {
         printf( "ouch! can't malloc!\n" );
         break;
      }
      mywork->jobnum = x;
      queue_put( &wq.work, ( node* )mywork );
   }

   printf("created & queued %d packets\n", numpackets );

   pthread_mutex_unlock( &wq.control.mutex );
   pthread_cond_broadcast( &wq.control.cond );

   printf( "sleeping %d seconds ...\n", sleeptime );
   sleep( sleeptime );
   printf( "deactivating work queue...\n" );
   control_deactivate( &wq.control );
   /* CLEANUP  */

   join_threads();
   cleanup_structs();

   printf("\n");

}
