/*
* Amazon FreeRTOS
* Copyright (C) 2017 Amazon.com, Inc. or its affiliates.  All Rights Reserved.
*
* Permission is hereby granted, free of charge, to any person obtaining a copy of
* this software and associated documentation files (the "Software"), to deal in
* the Software without restriction, including without limitation the rights to
* use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
* the Software, and to permit persons to whom the Software is furnished to do so,
* subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
* FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
* COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
* IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
* CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*
* http://aws.amazon.com/freertos
* http://www.FreeRTOS.org
*/

/**
* @file aws_iot_taskpool.c
* @brief Implements the user-facing functions of the MQTT library.
*/

/* Standard includes. */
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* Platform layer includes. */
#include "platform/aws_iot_threads.h"
#include "platform/aws_iot_clock.h"

/* Task Pool internal include. */
#include "private/aws_iot_taskpool_internal.h"

/* Validate Task Pool configuration settings. */
#if AWS_IOT_TASKPOOL_ENABLE_ASSERTS != 0 && AWS_IOT_TASKPOOL_ENABLE_ASSERTS != 1
#error "AWS_IOT_TASKPOOL_ENABLE_ASSERTS must be 0 or 1."
#endif
#if AWS_IOT_TASKPOOL_TEST != 0 && AWS_IOT_TASKPOOL_TEST != 1
#error "AWS_IOT_TASKPOOL_TEST must be 0 or 1."
#endif

/* Establish a few convenience macros */

/**
* @brief Checks a return result from internal and external APIs for failure.
*
*/
#define _TASKPOOL_FAILED(x)    ( ( x ) != 0)

/**
* @brief Checks a return result from internal and external APIs for success.
*
*/
#define _TASKPOOL_SUCCEEDED(x) ( ( x ) == 0)

/**
* @brief Enter a critical section by locking a mutex.
*
*/
#define _TASKPOOL_ENTER_CRITICAL_SECTION AwsIotMutex_Lock( &( ( ( AwsIotTaskPool_t *)pTaskPool )->lock ) )

/**
* @brief Exit a critical section by unlocking a mutex.
*
*/
#define _TASKPOOL_EXIT_CRITICAL_SECTION AwsIotMutex_Unlock( &( ( ( AwsIotTaskPool_t *)pTaskPool )->lock ) )

/**
* @brief Maximum semaphore value for wait operations.
*/
#define _TASKPOOL_MAX_SEM_VALUE 0xFFFF

/* ---------------------------------------------------------------------------------------------- */

/**
* @cond DOXYGEN_IGNORE
* Doxygen should ignore this section.
*
* @brief The system task pool handle for all libraries to use. 
* User application can use the system task pool as well knowing that the usage will be shared with 
* the system libraries as well. The system task pool needs to be initialized before any library is used or 
* before any code that posts jobs to the task pool runs. 
*/
AwsIotTaskPool_t _IotSystemTaskPool = { 0 };

/** @endcond */

/* ---------------------------------------------------------------------------------------------- */

/**
* @cond DOXYGEN_IGNORE
* Doxygen should ignore this section.
*/

/* -------------- Convenience functions to create/recycle/destroy jobs -------------- */

/** 
 * @brief Initializes one instance of a Task pool cache. 
 * 
 * @param[in] pCache The pre-allocated instance of the cache to initialize.
 */
static void _initWorkItemsCache( AwsIotTaskPoolCache_t * const pCache );

/**
* @brief Extracts and initializes one instance of a job from the cache or, if there is none available, it allocates and initialized a new one.
* 
* @param[in] pCache The instance of the cache to extract the job from.
* @param[in] userCallback The user callback to invoke.
* @param[in] pUserContext The user context to pass to the user callback as parameter.
*/
static AwsIotTaskPoolJob_t * _fetchOrAllocateWorkItem( AwsIotTaskPoolCache_t * const pCache, 
                                                            const IotTaskPoolRoutine_t userCallback, 
                                                            void * const pUserContext );

/**
* Recycles one instance of a job into the cache or, if the cache is full, it destroys it. 
* 
* @param[in] pCache The instance of the cache to recycle the job into.
* @param[in] pJob The job to recycle.
*
*/
static void _recycleWorkItem( AwsIotTaskPoolCache_t * const pCache, AwsIotTaskPoolJob_t * const pJob );

/**
* Destroys one instance of a job.
*
* @param[in] pJob The job to destroy.
*
*/
static void _destroyWorkItem( AwsIotTaskPoolJob_t * const pJob );

/* -------------- The worker thread procedure for a task pool thread -------------- */

/**
* The procedure for a task pool worker thread.
*
* @param[in] pUserContext The user context.
*
*/
static void _taskPoolWorker( void * pUserContext );

/* -------------- Convenience functions to create/initialize/destroy the task pool engine -------------- */

/**
* Initializes a pre-allocated instance of a Task Pool engine.
*
* @param[in] pInfo The initialization information for the Task Pool engine.
* @param[in] freeMemory A flag to mark the Task Pool as statically or dynamically allocated.
* @param[in] pTaskPool The pre-allocated instance of the Task Pool engine to initialize.
*
*/
static AwsIotTaskPoolError_t _initTaskPool( const AwsIotTaskPoolInfo_t * const pInfo, 
                                          bool freeMemory, 
                                          AwsIotTaskPool_t * const pTaskPool );

/**
* Initializes a pre-allocated instance of a Task Pool engine.
*
* @param[in] pInfo The initialization information for the Task Pool engine.
* @param[in] allocateTaskPool A flag to allocate the engine statically or dynamically.
* @param[in] pTaskPoolBuffer A storage to build the Task Pool when staic allocation is chosen.
* @param[out] pTaskPool The handle to the created Task Pool.
*
*/
static AwsIotTaskPoolError_t _createEngine( const AwsIotTaskPoolInfo_t * const pInfo, 
                                            bool allocateTaskPool,
                                            AwsIotTaskPool_t ** const ppTaskPool );

/**
* Destroys one instance of a Task Pool engine.
*
* @param[in] pTaskPool The Task Pool engine to destroy.
*
*/
static void _destroyTaskPool( AwsIotTaskPool_t * const pTaskPool );

/**
* Check for the exit condition.
*
* @param[in] pTaskPool The Task Pool engine to destroy.
*
*/
static bool _shutdownInProgress( const AwsIotTaskPool_t * const pTaskPool );

/**
* Set the exit condition.
*
* @param[in] pTaskPool The Task Pool engine to destroy.
*
*/
static void _signalShutdown( AwsIotTaskPool_t * const pTaskPool );
/** @endcond */

/* ---------------------------------------------------------------------------------------------- */

AwsIotTaskPool_t * AwsIotTaskPool_GetSystemTaskPool( )
{
    return &_IotSystemTaskPool;
}

AwsIotTaskPoolError_t AwsIotTaskPool_CreateSystemTaskPool( const AwsIotTaskPoolInfo_t * const pInfo )
{
    AwsIotTaskPoolError_t error = AwsIotTaskPool_CreateStatic( pInfo, &_IotSystemTaskPool );

    return error;
}

AwsIotTaskPoolError_t AwsIotTaskPool_CreateStatic( const AwsIotTaskPoolInfo_t * const pInfo, 
                                                   AwsIotTaskPool_t * pTaskPool )
{
    AwsIotTaskPoolError_t error;

    if ( pTaskPool == NULL )
    {
        error = AWS_IOT_TASKPOOL_BAD_PARAMETER;
    }
    else
    {
        error = _createEngine( pInfo, false, &pTaskPool );
    }

    return error;
}

AwsIotTaskPoolError_t AwsIotTaskPool_Create( const AwsIotTaskPoolInfo_t * const pInfo, AwsIotTaskPool_t ** const ppTaskPool )
{
    AwsIotTaskPoolError_t error = _createEngine( pInfo, true, ppTaskPool );

    return error;
}

AwsIotTaskPoolError_t AwsIotTaskPool_Destroy( AwsIotTaskPool_t * pTaskPool )
{
    uint32_t count;
    AwsIotTaskPoolError_t error = AWS_IOT_TASKPOOL_SUCCESS;

    /* Track how many threads the engine owns. */
    uint32_t activeThreads;

    /* Parameter checking. */
    if ( pTaskPool == NULL )
    {
        error = AWS_IOT_TASKPOOL_BAD_PARAMETER;
    }
    else
    {
        /* Destroying the task pool should be safe, and therefore we will grab the task pool lock.
         * No worker thread or application thread should access any data structure
         * in the Task Pool while the Task Pool is being destroyed. */
        _TASKPOOL_ENTER_CRITICAL_SECTION;
        {
            IotLink_t * pItemLink;
            bool executing = false;

            /* Record how many active threads in the engine. */
            activeThreads = pTaskPool->activeThreads;

            /* Destroying a Task pool happens in stages: first we lock the Task Pool mutex for safety, and then (1) we clear all queues.
             * After queues are cleared, (2) we set the exit condition and (3) wake up all active worker threads. Worker threads will observe
             * the exit condition and bail out without trying to pick up any further work. We will then release the mutex and (4) wait for all
             * worker threads to signal exit, before (5) destroying all engine data structures and release the associated memory.
             */

             /* (1) Clear the job queue. */
            do
            {
                pItemLink = NULL;

                pItemLink = IotQueue_Dequeue( &pTaskPool->dispatchQueue );

                if ( pItemLink != NULL )
                {
                    AwsIotTaskPoolJob_t * pJob = IotLink_Container( AwsIotTaskPoolJob_t, pItemLink, link );

                    _destroyWorkItem( pJob );
                }

            } while ( pItemLink );

            /* (1) Clear the job cache. */
            do
            {
                pItemLink = NULL;

                pItemLink = IotListDouble_RemoveHead( &( pTaskPool->jobsCache.freeList ) );

                if ( pItemLink != NULL )
                {
                    AwsIotTaskPoolJob_t * pJob = IotLink_Container( AwsIotTaskPoolJob_t, pItemLink, link );

                    _destroyWorkItem( pJob );
                }

            } while ( pItemLink );

            /* (2) Set the exit condition. */
            _signalShutdown( pTaskPool );

            /* (3) Broadcast to all active threads to wake-up. Active threads do check the exit condition right after wakein up. */
            for ( count = 0; count < activeThreads; ++count )
            {
                AwsIotSemaphore_Post( &pTaskPool->dispatchSignal );
            }
        }
        _TASKPOOL_EXIT_CRITICAL_SECTION;

        /* (4) Wait for all active threads to reach the end of their life-span. */
        for ( count = 0; count < activeThreads; ++count )
        {
            AwsIotSemaphore_Wait( &pTaskPool->startStopSignal );
        }

        while ( pTaskPool->activeThreads != 0 )
        {
        }

        AwsIotTaskPool_Assert( pTaskPool->activeThreads == 0 );
        AwsIotTaskPool_Assert( AwsIotSemaphore_GetCount( &pTaskPool->startStopSignal ) == 0 );

        /* (5) Destroy all signaling objects. */
        _destroyTaskPool( pTaskPool );
    }

    return error;
}

AwsIotTaskPoolError_t AwsIotTaskPool_SetMaxThreads( AwsIotTaskPool_t * pTaskPool, uint32_t maxThreads )
{
    AwsIotTaskPoolError_t error = AWS_IOT_TASKPOOL_SUCCESS;

    /* Parameter checking. */
    if ( pTaskPool == NULL ||
         pTaskPool->minThreads > maxThreads ||
         maxThreads < 1 )
    {
        error = AWS_IOT_TASKPOOL_BAD_PARAMETER;
    }
    else
    {
        _TASKPOOL_ENTER_CRITICAL_SECTION;
        {
            /* Bail out early if this task pool is shutting down. */
            if ( _shutdownInProgress( pTaskPool ) )
            {
                error = AWS_IOT_TASKPOOL_SHUTDOWN_IN_PROGRESS;
            }
            else
            {
                uint32_t currentMaxThreads = pTaskPool->maxThreads;

                /* Reset the max threads counter. */
                pTaskPool->maxThreads = maxThreads;

                /* If the number of maximum threads in the pool is set to be smaller than the current value,
                * then we need to signal all redundant to exit.
                */
                if ( maxThreads < currentMaxThreads )
                {
                    uint32_t count = currentMaxThreads - maxThreads;

                    while ( count-- > 0 )
                    {
                        AwsIotSemaphore_Post( &pTaskPool->dispatchSignal );
                    }
                }
            }
        }
        _TASKPOOL_EXIT_CRITICAL_SECTION;
    }

    return error;
}

AwsIotTaskPoolError_t AwsIotTaskPool_CreateJobStatic(
    const AwsIotTaskPool_t * pTaskPool,
    const IotTaskPoolRoutine_t userCallback,
    void * const pUserContext,
    AwsIotTaskPoolJob_t * const pJob )
{
    AwsIotTaskPoolError_t error = AWS_IOT_TASKPOOL_SUCCESS;

    /* Parameter checking. */
    if ( pTaskPool == NULL || 
         userCallback == NULL || 
         pJob == NULL )
    {
        error = AWS_IOT_TASKPOOL_BAD_PARAMETER;
    }
    else
    {
        /* Build a job around the user-provided storage. */
        pJob->link.pNext = NULL;
        pJob->link.pPrevious = NULL;
        pJob->userCallback = userCallback;
        pJob->pUserContext = pUserContext;
        pJob->statusAndFlags = AWS_IOT_TASKPOOL_STATUS_READY | AWS_IOT_TASK_POOL_INTERNAL_STATIC;

        if ( AwsIotSemaphore_Create( &pJob->waitHandle, 0, 1 ) == false )
        {
            AwsIotLogError( "Failed to allocate wait handle." );

            error = AWS_IOT_TASKPOOL_NO_MEMORY;
        }
    }

    return error;
}

AwsIotTaskPoolError_t AwsIotTaskPool_CreateJob( const AwsIotTaskPool_t * pTaskPool, 
                                                const IotTaskPoolRoutine_t userCallback, 
                                                void * const pUserContext,
                                                AwsIotTaskPoolJob_t ** const pJob )
{
    AwsIotTaskPoolError_t error = AWS_IOT_TASKPOOL_SUCCESS;

    /* Parameter checking. */
    if ( pTaskPool == NULL ||
         userCallback == NULL || 
         pJob == NULL )
    {
        error = AWS_IOT_TASKPOOL_BAD_PARAMETER;
    }
    else
    {
        AwsIotTaskPoolJob_t * pJobTemp;

        _TASKPOOL_ENTER_CRITICAL_SECTION;
        {

            /* Build a job. */
            pJobTemp = _fetchOrAllocateWorkItem( &(( AwsIotTaskPool_t * )pTaskPool)->jobsCache, userCallback, pUserContext );

            if ( pJobTemp == NULL )
            {
                AwsIotLogInfo( "Failed to allocate a job." );

                error = AWS_IOT_TASKPOOL_NO_MEMORY;
            }
            else
            {
                /* A newly created job is 'ready' to be scheduled. */
                AwsIotTaskPool_Assert( ( pJobTemp->statusAndFlags & AWS_IOT_TASKPOOL_STATUS_READY ) == AWS_IOT_TASKPOOL_STATUS_READY );
            }
        }
        _TASKPOOL_EXIT_CRITICAL_SECTION;

        *pJob = pJobTemp;
    }

    return error;
}

AwsIotTaskPoolError_t AwsIotTaskPool_RecycleJob( const AwsIotTaskPool_t * pTaskPool, 
                                                 AwsIotTaskPoolJob_t * const pJob )
{
    AwsIotTaskPoolError_t error = AWS_IOT_TASKPOOL_SUCCESS;

    /* Parameter checking. */
    if ( pTaskPool == NULL ||
         pJob == NULL)
    {
        error = AWS_IOT_TASKPOOL_BAD_PARAMETER;
    }
    else
    {
        /* Do not recycle statically allocated jobs. */
        if ( ( pJob->statusAndFlags & AWS_IOT_TASK_POOL_INTERNAL_STATIC ) == AWS_IOT_TASK_POOL_INTERNAL_STATIC )
        {
            AwsIotLogWarn( "Attempt to recycle a statically allocated job." );

            error = AWS_IOT_TASKPOOL_ILLEGAL_OPERATION;
        }
        else
        {
            _TASKPOOL_ENTER_CRITICAL_SECTION;
            {
                /* Do not recycle or destroy a job linked in a queue. */
                if ( IotLink_IsLinked( &pJob->link ) )
                {
                    AwsIotLogWarn( "Attempt to recycle a job that is scheduled for execution or recycled already." );

                    error = AWS_IOT_TASKPOOL_ILLEGAL_OPERATION;
                }
                else
                {
                    _recycleWorkItem( &( ( AwsIotTaskPool_t * )pTaskPool )->jobsCache, pJob );
                }
            }
            _TASKPOOL_EXIT_CRITICAL_SECTION;
        }
    }

    return error;
}

AwsIotTaskPoolError_t AwsIotTaskPool_DestroyJob( const AwsIotTaskPool_t * pTaskPool, 
                                                  AwsIotTaskPoolJob_t * const pJob )
{
    AwsIotTaskPoolError_t error = AWS_IOT_TASKPOOL_SUCCESS;

    /* Parameter checking. */
    if ( pTaskPool == NULL || 
         pJob == NULL )
    {
        error = AWS_IOT_TASKPOOL_BAD_PARAMETER;
    }
    else
    {
        _TASKPOOL_ENTER_CRITICAL_SECTION;
        {

            /* Do not destroy a job in the dispatch queue or the recycle queue. */
            if ( IotLink_IsLinked( &pJob->link ) )
            {
                AwsIotLogWarn( "Attempt to destroy a job that is part of a queue." );

                error = AWS_IOT_TASKPOOL_ILLEGAL_OPERATION;
            }
            else
            {
                _destroyWorkItem( pJob );
            }
        }
        _TASKPOOL_EXIT_CRITICAL_SECTION;
    }

    return error;
}

AwsIotTaskPoolError_t AwsIotTaskPool_Schedule( AwsIotTaskPool_t * const pTaskPool, 
                                               AwsIotTaskPoolJob_t * const pJob )
{
    AwsIotTaskPoolError_t error = AWS_IOT_TASKPOOL_SUCCESS;

    /* Parameter checking. */
    if ( pTaskPool == NULL ||
         pJob == NULL )
    {
        error = AWS_IOT_TASKPOOL_BAD_PARAMETER;
    }
    /* Bail out early if this task pool is shutting down. */
    else if ( _shutdownInProgress( pTaskPool ) )
    {
        AwsIotLogWarn( "Attempt to schedule a job while shutdown is in progress." );

        error = AWS_IOT_TASKPOOL_SHUTDOWN_IN_PROGRESS;
    }
    else
    {
        _TASKPOOL_ENTER_CRITICAL_SECTION;
        {
            /* Bail out early if this task pool is shutting down. */
            if ( _shutdownInProgress( pTaskPool ) )
            {
                AwsIotLogInfo( "Attempt to schedule a job while shutdown is in progress." );

                error = AWS_IOT_TASKPOOL_SHUTDOWN_IN_PROGRESS;
            }
            else if ( IotLink_IsLinked( &pJob->link ) )
            {
                AwsIotLogWarn( "Attempt to schedule a job in the dispatch queue." );

                error = AWS_IOT_TASKPOOL_ILLEGAL_OPERATION;
            }
            else
            {
                /* Append the job to the dispatch queue. */
                IotQueue_Enqueue( &pTaskPool->dispatchQueue, &pJob->link );

                /* Update the job status to 'scheduled'. */
                pJob->statusAndFlags &= ~AWS_IOT_TASKPOOL_STATUS_MASK;
                pJob->statusAndFlags |= AWS_IOT_TASKPOOL_STATUS_SCHEDULED;

                /* Signal a worker to pick up the job. */
                AwsIotSemaphore_Post( &pTaskPool->dispatchSignal );

                /* If all threads are busy, try and create a new one. Failing to create a new thread
                 * only has performance implications on correctly exeuting th scheduled job.
                 */
                uint32_t activeThreads = pTaskPool->activeThreads;

                if ( activeThreads == pTaskPool->busyThreads )
                {
                    if ( activeThreads < pTaskPool->maxThreads )
                    {
                        AwsIotLogInfo( "Growing a Task pool with a new worker thread..." );

                        /* TODO TODO TODO if ( AwsIot_CreateDetachedThread( pTaskPool->stackSize, pTaskPool->priority, _taskPoolWorker, pTaskPool ) ) */
                        if ( AwsIot_CreateDetachedThread( _taskPoolWorker, pTaskPool ) )
                        {
                            AwsIotSemaphore_Wait( &pTaskPool->startStopSignal );

                            pTaskPool->activeThreads++;
                        }
                        else
                        {
                            /* Failure to create a worker thread does not hinder functional correctness, but rather just responsiveness. */
                            AwsIotLogWarn( "Task pool failed to create a worker thread." );
                        }
                    }
                }
            }
        }
        _TASKPOOL_EXIT_CRITICAL_SECTION;
    }

    return error;
}

AwsIotTaskPoolError_t AwsIotTaskPool_Wait( const AwsIotTaskPool_t * pTaskPool, AwsIotTaskPoolJob_t * const pJob )
{
    AwsIotTaskPoolError_t error = AwsIotTaskPool_TimedWait( pTaskPool, pJob, UINT32_MAX );

    return error;
}

AwsIotTaskPoolError_t AwsIotTaskPool_TimedWait( const AwsIotTaskPool_t * pTaskPool, AwsIotTaskPoolJob_t * const pJob, uint64_t timeoutMs )
{
    AwsIotTaskPoolError_t error = AWS_IOT_TASKPOOL_SUCCESS;
    bool waitable = false;

    /* Parameter checking. */
    if ( pTaskPool == NULL || 
         pJob == NULL )
    {
        error = AWS_IOT_TASKPOOL_BAD_PARAMETER;
    }
    /* Bail out early if this task pool is shutting down. */
    else if ( _shutdownInProgress( pTaskPool ) )
    {
        AwsIotLogWarn( "Attempt to wait a job while shutdown is in progress." );

        error = AWS_IOT_TASKPOOL_SHUTDOWN_IN_PROGRESS;
    }
    else
    {
        _TASKPOOL_ENTER_CRITICAL_SECTION;
        {
            /* Check again for shut down. */
            if ( _shutdownInProgress( pTaskPool ) )
            {
                AwsIotLogWarn( "Attempt to wait a job while shutdown is in progress." );

                error = AWS_IOT_TASKPOOL_SHUTDOWN_IN_PROGRESS;
            }
            else
            {
                AwsIotTaskPoolJobStatus_t currentStatus;

                currentStatus = pJob->statusAndFlags & AWS_IOT_TASKPOOL_STATUS_MASK;

                /* Only jobs that are schedulable, scheduled, or executing can be waited on. */
                switch ( currentStatus )
                {
                case AWS_IOT_TASKPOOL_STATUS_READY:
                case AWS_IOT_TASKPOOL_STATUS_SCHEDULED:
                case AWS_IOT_TASKPOOL_STATUS_EXECUTING:
                    ( (AwsIotTaskPoolJob_t * )pJob )->statusAndFlags |= AWS_IOT_TASK_POOL_INTERNAL_MARKED_FOR_WAIT; /* The wait flag will prevent early recycling of the job. */
                    waitable = true;
                    break;

                case AWS_IOT_TASKPOOL_STATUS_COMPLETED:
                case AWS_IOT_TASKPOOL_STATUS_CANCELED:
                    break;

                default:
                    break;
                }
            }
        }
        _TASKPOOL_EXIT_CRITICAL_SECTION;
    }

    /* Wait on the job. */
    if ( waitable )
    {
        bool res = AwsIotSemaphore_TimedWait( &pJob->waitHandle, timeoutMs );

        /* Clear the wait flag. */
        ( ( AwsIotTaskPoolJob_t * )pJob )->statusAndFlags &= ~AWS_IOT_TASK_POOL_INTERNAL_MARKED_FOR_WAIT;

        /* Return the appropriate error for the failure case. */
        if ( res == false )
        {
            error = AWS_IOT_TASKPOOL_TIMEDOUT;
        }
    }

    return error;
}

AwsIotTaskPoolError_t AwsIotTaskPool_GetStatus( const AwsIotTaskPoolJob_t * pJob, AwsIotTaskPoolJobStatus_t * const pStatus )
{
    AwsIotTaskPoolError_t error = AWS_IOT_TASKPOOL_SUCCESS;

    /* Parameter checking. */
    if ( pJob == NULL || 
         pStatus == NULL )
    {
        error = AWS_IOT_TASKPOOL_BAD_PARAMETER;
    }
    else
    {
        *pStatus = ( ( AwsIotTaskPoolJob_t * )pJob )->statusAndFlags & AWS_IOT_TASKPOOL_STATUS_MASK;
    }

    return error;
}

AwsIotTaskPoolError_t AwsIotTaskPool_TryCancel( const AwsIotTaskPool_t * pTaskPool, 
                                                AwsIotTaskPoolJob_t * const pJob, 
                                                AwsIotTaskPoolJobStatus_t * const pStatus )
{
    AwsIotTaskPoolError_t error = AWS_IOT_TASKPOOL_SUCCESS;

    /* Parameter checking. */
    if ( pTaskPool == NULL ||
         pJob == NULL )
    {
        error = AWS_IOT_TASKPOOL_BAD_PARAMETER;
    }
    /* Bail out early if this task pool is shutting down. */
    else if ( _shutdownInProgress( pTaskPool ) )
    {
        AwsIotLogWarn( "Attempt to cancel a job while shutdown is in progress." );

        error = AWS_IOT_TASKPOOL_SHUTDOWN_IN_PROGRESS;
    }
    else
    {
        *pStatus = AWS_IOT_TASKPOOL_STATUS_UNDEFINED;

        _TASKPOOL_ENTER_CRITICAL_SECTION;
        {
            /* Check again if this task pool is shutting down. */
            if ( _shutdownInProgress( pTaskPool ) )
            {
                AwsIotLogWarn( "Attempt to cancel a job while shutdown is in progress." );

                error = AWS_IOT_TASKPOOL_SHUTDOWN_IN_PROGRESS;
            }
            else 
            {
                /* We can only cancel jobs that are either 'ready' (waiting to be scheduled) or 'scheduled'. */
                {
                    bool cancelable = false;

                    /* Register the current status. */
                    AwsIotTaskPoolJobStatus_t currentStatus = pJob->statusAndFlags & AWS_IOT_TASKPOOL_STATUS_MASK;

                    switch ( currentStatus )
                    {
                    case AWS_IOT_TASKPOOL_STATUS_READY:
                    case AWS_IOT_TASKPOOL_STATUS_SCHEDULED:
                        cancelable = true;
                        break;

                    case AWS_IOT_TASKPOOL_STATUS_EXECUTING:
                    case AWS_IOT_TASKPOOL_STATUS_COMPLETED:
                    case AWS_IOT_TASKPOOL_STATUS_CANCELED:
                        AwsIotLogWarn( "Attempt to cancel a job that is already, executing, completed, or canceled." );
                        break;

                    default:
                        AwsIotLogError( "Attempt to cancel a job with an undefined state." );
                        break;
                    }

                    /* Update the returned status to the current status of the job. */
                    if ( pStatus != NULL )
                    {
                        *pStatus = currentStatus;
                    }

                    if ( cancelable )
                    {
                        /* Update the status of the job. */
                        ( ( AwsIotTaskPoolJob_t * )pJob )->statusAndFlags &= ~AWS_IOT_TASKPOOL_STATUS_MASK;
                        ( ( AwsIotTaskPoolJob_t * )pJob )->statusAndFlags |= AWS_IOT_TASKPOOL_STATUS_CANCELED;

                        /* If the job is cancelable and still in the dispatch queue, then unlink it and signal any waiting threads. */
                        if ( IotLink_IsLinked( &pJob->link ) )
                        {
                            /* If the job is cancelable, it must be in the dispatch queue. */
                            AwsIotTaskPool_Assert( currentStatus == AWS_IOT_TASKPOOL_STATUS_SCHEDULED );

                            IotQueue_Remove( &pJob->link );

                            AwsIotSemaphore_Post( &pJob->waitHandle );
                        }
                    }
                    else
                    {
                        error = AWS_IOT_TASKPOOL_FAILED;
                    }
                }
            }
        }
        _TASKPOOL_EXIT_CRITICAL_SECTION;
    }

    return error;
}

/* ---------------------------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */

/**
* @cond DOXYGEN_IGNORE
* Doxygen should ignore this section.
*/

static AwsIotTaskPoolError_t _createEngine( const AwsIotTaskPoolInfo_t * const pInfo, bool allocateTaskPool, AwsIotTaskPool_t ** const ppTaskPool )
{
    uint32_t count;
    AwsIotTaskPoolError_t error = AWS_IOT_TASKPOOL_SUCCESS;

    /* Check input values for consistency. */
    if ( pInfo == NULL ||
         ppTaskPool == NULL ||
         pInfo->minThreads > pInfo->maxThreads    ||
         pInfo->minThreads < 1 ||
         pInfo->maxThreads < 1 )
    {
        error = AWS_IOT_TASKPOOL_BAD_PARAMETER;
    }

    /* Start creating the pTaskPool. */
    if ( _TASKPOOL_SUCCEEDED( error ) )
    {
        AwsIotTaskPool_t * pTaskPool;

        if ( allocateTaskPool )
        {
            /* The pTaskPool is allocated on the heap. */
            pTaskPool = ( AwsIotTaskPool_t * )AwsIotTaskPool_Malloc( sizeof( AwsIotTaskPool_t ) );

            if ( pTaskPool == NULL )
            {
                error = AWS_IOT_TASKPOOL_NO_MEMORY;
            }
        }
        else
        {
            pTaskPool = *ppTaskPool;
        }

        if ( _TASKPOOL_SUCCEEDED( error ) )
        {
            /* Initialize all internal data structure prior to creating all threads. */
            error = _initTaskPool( pInfo, allocateTaskPool, pTaskPool );

            if ( _TASKPOOL_SUCCEEDED( error ) )
            {
                uint32_t threadsCreated;

                AwsIotTaskPool_Assert( pInfo->minThreads == pTaskPool->minThreads );
                AwsIotTaskPool_Assert( pInfo->maxThreads == pTaskPool->maxThreads );

                /* The task pool will initialize the minimum number of threads reqeusted by the user upon start. */
                /* When a thread is created, it will signal a semaphore to signify that it is about to wait on incoming */
                /* jobs. A thread can be woken up for exit or for new jobs only at that point in time.  */
                /* The exit condition is setting the maximum number of threads to 0. */

                /* Create the minimum number of threads specified by the user, and if one fails shutdown and return error. */
                for ( threadsCreated = 0; threadsCreated < pTaskPool->minThreads; )
                {
                    /* Create one thread. */
                    /* TODO TODO TODO if ( AwsIot_CreateDetachedThread( pTaskPool->stackSize, pTaskPool->priority, _taskPoolWorker, pTaskPool ) == false ) */
                    if ( AwsIot_CreateDetachedThread( _taskPoolWorker, pTaskPool ) == false )
                    {
                        AwsIotLogError( "Could not create worker thread! Exiting..." );

                        /* If creating one thread fails, set error condition and exit the loop. */
                        error = AWS_IOT_TASKPOOL_NO_MEMORY;
						
                        break;
                    }

                    /* Upon succesfull thread creation, increase the number of active threads. */
                    pTaskPool->activeThreads++;

                    ++threadsCreated;
                }

                /* Wait for threads to be ready to wait on the condition, so that threads are actually able to receive messages. */
                for ( count = 0; count < threadsCreated; ++count )
                {
                    AwsIotSemaphore_Wait( &pTaskPool->startStopSignal );
                }

                /* In case of failure, wait on the created threads to exit. */
                if ( _TASKPOOL_FAILED( error ) )
                {
                    /* Set the exit condition for the newly created threads. */
                    _signalShutdown( pTaskPool );

                    /* Signal all threads to exit. */
                    for ( count = 0; count < threadsCreated; ++count )
                    {
                        AwsIotSemaphore_Wait( &pTaskPool->startStopSignal );
                    }

                    _destroyTaskPool( pTaskPool );
                }
                else
                {
                    *ppTaskPool = pTaskPool;
                }
            }
        }
    }

    return error;
}

static AwsIotTaskPoolError_t _initTaskPool( const AwsIotTaskPoolInfo_t * const pInfo, bool freeMemory, AwsIotTaskPool_t * const pTaskPool )
{
    AwsIotTaskPoolError_t error = AWS_IOT_TASKPOOL_SUCCESS;

    bool semStartStopInit = false;
    bool lockInit = false;
    bool semDispatchInit = false;

    /* Zero out all data structures. */
    memset( ( void * )pTaskPool, 0x00, sizeof( AwsIotTaskPool_t ) );

    pTaskPool->freeMemory = freeMemory;

    /* Initialize a job data structures that require no de-initialization.
    * All other data structures carry a value of 'NULL' before initailization.
    */
    IotQueue_Create( &pTaskPool->dispatchQueue );
    
    pTaskPool->minThreads = pInfo->minThreads;
    pTaskPool->maxThreads = pInfo->maxThreads;
    pTaskPool->stackSize = pInfo->stackSize;
    pTaskPool->priority = pInfo->priority;

    _initWorkItemsCache( &pTaskPool->jobsCache );
    
    /* Initialize the semaphore to ensure all threads have started. */
    if ( AwsIotSemaphore_Create( &pTaskPool->startStopSignal, 0, _TASKPOOL_MAX_SEM_VALUE ) )
    {
        semStartStopInit = true;

        if ( AwsIotMutex_Create( &pTaskPool->lock ) )
        {
            lockInit = true;

            /* Initialize the semaphore for waiting for incoming work. */
            if ( AwsIotSemaphore_Create( &pTaskPool->dispatchSignal, 0, _TASKPOOL_MAX_SEM_VALUE ) )
            {
                semDispatchInit = true;
            }
            else
            {
                error = AWS_IOT_TASKPOOL_NO_MEMORY;
            }
        }
        else
        {
            error = AWS_IOT_TASKPOOL_NO_MEMORY;
        }
    }
    else
    {
        error = AWS_IOT_TASKPOOL_NO_MEMORY;
    }

    if ( _TASKPOOL_FAILED( error ) )
    {
        if ( semStartStopInit )
        {
            AwsIotSemaphore_Destroy( &pTaskPool->startStopSignal );
        }
        if ( lockInit )
        {
            AwsIotMutex_Destroy( &pTaskPool->lock );
        }
        if ( semDispatchInit )
        {
            AwsIotSemaphore_Destroy( &pTaskPool->dispatchSignal );
        }
    }

    return error;
}

static void _destroyTaskPool( AwsIotTaskPool_t * const pTaskPool )
{
    AwsIotSemaphore_Destroy( &pTaskPool->startStopSignal );

    AwsIotMutex_Destroy( &pTaskPool->lock );

    AwsIotSemaphore_Destroy( &pTaskPool->dispatchSignal );

    if ( pTaskPool->freeMemory == true )
    {
        AwsIotTaskPool_Free( pTaskPool );
    }
}

/* ---------------------------------------------------------------------------------------------- */

static void _taskPoolWorker( void * pUserContext )
{
    /* Extract pTaskPool pointer from context. */
    AwsIotTaskPool_Assert( pUserContext != NULL );
    AwsIotTaskPool_t * pTaskPool = ( AwsIotTaskPool_t * )pUserContext;

    /* Signal that this worker completed initialization and it is ready to receive notifications. */
    AwsIotSemaphore_Post( &pTaskPool->startStopSignal );

    /* OUTER LOOP: it controls the lifetiem of the worker thread: exit condition for a worker thread
    * is setting maxThreads to zero. A worker thread is running until the maximum number of allowed
    * threads is not zero and the active threads are less than the maximum number of allowed threads.
    */
    while ( pTaskPool->activeThreads <= pTaskPool->maxThreads )
    {
        IotLink_t * pFirst = NULL;
        IotLink_t * pNext = NULL;
        AwsIotTaskPoolJob_t * pJob = NULL;

        /* Acquire the lock to check the exit condition, and release the lock if the exit condition is verified,
        * or before waiting for incoming notifications.
        */
        _TASKPOOL_ENTER_CRITICAL_SECTION;
        {
            /* If the exit condition is verified, update the number of active threads and exit the loop. */
            if ( _shutdownInProgress( pTaskPool ) )
            {
                AwsIotLogDebug( "Worker thread exiting because exit condition was set." );

                /* Release the lock before checking the exit condition. */
                _TASKPOOL_EXIT_CRITICAL_SECTION;

                /* Abandon the OUTER LOOP. */
                break;
            }
        }
        _TASKPOOL_EXIT_CRITICAL_SECTION;

        /* Wait on incoming notifications... */
        AwsIotSemaphore_Wait( &pTaskPool->dispatchSignal );

        /* .. upon receiving a notification, grab the lock... */
        _TASKPOOL_ENTER_CRITICAL_SECTION;
        {
            /* If the exit condition is verified, update the number of active threads and exit the loop.
            * This can happen when the Task Pool is being destroyed.
            */
            if ( _shutdownInProgress( pTaskPool ) )
            {
                AwsIotLogDebug( "Worker thread exiting because exit condition was set." );

                _TASKPOOL_EXIT_CRITICAL_SECTION;

                /* Abandon the OUTER LOOP. */
                break;
            }

            /* Dequeue one job in FIFO order. */
            pFirst = IotQueue_Dequeue( &pTaskPool->dispatchQueue );

            /* If there is indeed a job, then update status under lock, and release the lock before processing the job. */
            if ( pFirst != NULL )
            {
                /* Update the number of busy threads, so new requests can be served by creating new threads, up to maxThreads. */
                pTaskPool->busyThreads++;

                /* Extract the job from its link. */
                pJob = IotLink_Container( AwsIotTaskPoolJob_t, pFirst, link );

                /* Update status to 'executing'. */
                pJob->statusAndFlags &= ~AWS_IOT_TASKPOOL_STATUS_MASK;
                pJob->statusAndFlags |= AWS_IOT_TASKPOOL_STATUS_EXECUTING;
				
                /* Check if there is another job in the queue, and if there is one, signal the task pool. */
                pNext = IotQueue_Peek( &pTaskPool->dispatchQueue );

                if ( pNext != NULL )
                {
                    AwsIotSemaphore_Post( &pTaskPool->dispatchSignal );
                }
            }
        }
        _TASKPOOL_EXIT_CRITICAL_SECTION;

        /* INNER LOOP: it controls the execution of jobs: the exit condition is the lack of a job to execute. */
        while ( pJob != NULL )
        {
			/* Record callback, so job can be re-used in the callback. */
			IotTaskPoolRoutine_t userCallback = pJob->userCallback;
			
            /* Process the job by invoking the associated callback with the user context.
            * This task pool thread will not be available until the user callback returns.
            */
            {
                userCallback( pTaskPool, pJob, pJob->pUserContext );
            }

            /* Acquire the lock before updating the job status. */
            _TASKPOOL_ENTER_CRITICAL_SECTION;
            {
                /* Mark the job as 'completed', and signal completion on the associated semaphore. */
                pJob->statusAndFlags &= ~AWS_IOT_TASKPOOL_STATUS_MASK;
                pJob->statusAndFlags |= AWS_IOT_TASKPOOL_STATUS_COMPLETED;

                /* Signal the completion wait handle. */
                AwsIotSemaphore_Post( &pJob->waitHandle );

                /* Try and dequeue the next job in the dispatch queue. */
                {
                    IotLink_t * pItem = NULL;

                    /* Dequeue the next job from the dispatch queue. */
                    pItem = IotQueue_Dequeue( &pTaskPool->dispatchQueue );

                    /* If there is no job left in the dispatch queue, update the worker status and leave. */
                    if ( pItem == NULL )
                    {
                        /* Update the busy threads value. */
                        pTaskPool->busyThreads--;

                        _TASKPOOL_EXIT_CRITICAL_SECTION;

                        /* Abandon the INNER LOOP. Execution will tranfer back to the OUTER LOOP condition. */
                        break;
                    }
                    else
                    {
                        pJob = IotLink_Container( AwsIotTaskPoolJob_t, pItem, link );
                    }
                }

                pJob->statusAndFlags &= ~AWS_IOT_TASKPOOL_STATUS_MASK;
                pJob->statusAndFlags |= AWS_IOT_TASKPOOL_STATUS_EXECUTING;
            }
            _TASKPOOL_EXIT_CRITICAL_SECTION;
        }
    }

    AwsIotLogDebug( "Worker thread exiting because maximum quota was exceeded." );

    _TASKPOOL_ENTER_CRITICAL_SECTION;
    {
        /* Signal that this worker is exiting. */
        AwsIotSemaphore_Post( &pTaskPool->startStopSignal );

        pTaskPool->activeThreads--;
    }
    _TASKPOOL_EXIT_CRITICAL_SECTION;
}

/* ---------------------------------------------------------------------------------------------- */

static void _initWorkItemsCache( AwsIotTaskPoolCache_t * const pCache )
{
    IotQueue_Create( &pCache->freeList );

    pCache->freeCount = 0;
}

static AwsIotTaskPoolJob_t * _fetchOrAllocateWorkItem( AwsIotTaskPoolCache_t * const pCache, const IotTaskPoolRoutine_t userCallback, void * const pUserContext )
{
    AwsIotTaskPoolJob_t * pJob = NULL;
    IotLink_t * pLink;

    IotContainers_ForEach( &pCache->freeList, pLink )
    {
        AwsIotTaskPoolJob_t * pTemp = IotLink_Container( AwsIotTaskPoolJob_t, pLink, link );

        /* Only use a job that is not marked for an impending wait operation. */
        if ( ( pTemp->statusAndFlags & AWS_IOT_TASK_POOL_INTERNAL_MARKED_FOR_WAIT ) == 0 )
        {
            pJob = pTemp;

            AwsIotTaskPool_Assert( IotLink_IsLinked( pLink ) );

            IotQueue_Remove( pLink );

            break;
        }
    }

    /* If there is no available job in the cache, then allocate one. */
    if ( pJob == NULL )
    {
        pJob = ( AwsIotTaskPoolJob_t * )AwsIotTaskPool_Malloc( sizeof( AwsIotTaskPoolJob_t ) );

        if ( pJob != NULL )
        {
            pJob->link.pNext = NULL;
            pJob->link.pPrevious = NULL;

            if ( AwsIotSemaphore_Create( &pJob->waitHandle, 0, 1 ) == false )
            {
                AwsIotLogInfo( "Failed to allocate wait handle." );

                AwsIotTaskPool_Free( pJob );

                pJob = NULL;
            }
        }
        else
        {
            AwsIotLogInfo( "Failed to allocate job." );
        }
    }
    /* If there was a job in the cache, then make sure we keep the counters up-to-date. */
    else
    {
        AwsIotTaskPool_Assert( pCache->freeCount > 0 );

        pCache->freeCount--;
    }

    /* Initialize all members. */
    if ( pJob != NULL )
    {
        pJob->userCallback = userCallback;
        pJob->pUserContext = pUserContext;
        pJob->statusAndFlags = AWS_IOT_TASKPOOL_STATUS_READY;
    }

    return pJob;
}

static void _recycleWorkItem( AwsIotTaskPoolCache_t * const pCache, AwsIotTaskPoolJob_t * const pJob )
{
    bool shouldRecycle = false;

    /* We should never try and recycling a job that is linked into some queue. */
    AwsIotTaskPool_Assert( IotLink_IsLinked( &pJob->link ) == false );

    /* We will always recycle the job if is marked for an impending wait operation, so the wait 
     * can complete safely. The cache may grow larger than the 'recycle limit', but it will shrink down 
     * again as the 'wait' operation completes.
     */
    if ( ( pJob->statusAndFlags & AWS_IOT_TASK_POOL_INTERNAL_MARKED_FOR_WAIT ) == AWS_IOT_TASK_POOL_INTERNAL_MARKED_FOR_WAIT )
    {
        shouldRecycle = true;
    }
    /* Also, we will recycle the job if there is space in the cache. */
    else if ( pCache->freeCount < AWS_IOT_TASKPOOL_WORKTEIM_RECYCLE_LIMIT )
    {
        shouldRecycle = true;
    }

    /* Push the job back to the cache, or, if the cache is full, dispose of it. */
    if ( shouldRecycle )
    {
        /* If the job is NOT marked for wait, decrease the wait count artificially. */
        if ( ( pJob->statusAndFlags & AWS_IOT_TASK_POOL_INTERNAL_MARKED_FOR_WAIT ) == 0 )
        {
            while ( AwsIotSemaphore_GetCount ( &pJob->waitHandle ) > 0 )
            {
                AwsIotSemaphore_Wait( &pJob->waitHandle );
            }
        }
		
        pJob->userCallback = NULL;
        pJob->pUserContext = NULL;

        IotListDouble_InsertTail( &pCache->freeList, &pJob->link );

        pCache->freeCount++;

        AwsIotTaskPool_Assert( pCache->freeCount >= 1 );
    }
    else
    {
        _destroyWorkItem( pJob );
    }
}

static void _destroyWorkItem( AwsIotTaskPoolJob_t * const pJob )
{
    /* Cannot destroy statically allocated jobs, but can de-allocated the wait handle. */

    /* Dispose of the wait handle first. */
    AwsIotSemaphore_Destroy( &pJob->waitHandle );

    /* Dispose of dynamically allocated jobs. */
    if ( ( pJob->statusAndFlags & AWS_IOT_TASK_POOL_INTERNAL_STATIC ) == 0 )
    {
        AwsIotTaskPool_Free( pJob );
    }
}

/* ---------------------------------------------------------------------------------------------- */

static bool _shutdownInProgress( const AwsIotTaskPool_t * const pTaskPool )
{
    return ( pTaskPool->maxThreads == 0 );
}

static void _signalShutdown( AwsIotTaskPool_t * const pTaskPool )
{
    pTaskPool->maxThreads = 0;
}

/** @endcond */
