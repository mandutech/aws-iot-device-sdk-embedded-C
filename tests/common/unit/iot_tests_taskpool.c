/*
 * Copyright (C) 2019 Amazon.com, Inc. or its affiliates.  All Rights Reserved.
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
 */

/**
 * @file iot_tests_taskpool.c
 * @brief Tests for task pool.
 */

/* Build using a config header, if provided. */
#ifdef IOT_CONFIG_FILE
    #include IOT_CONFIG_FILE
#endif

/* Standard includes. */
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* POSIX includes. */
#include <time.h>

/* Platform layer includes. */
#include "platform/aws_iot_threads.h"

/* MQTT internal include. */
#include "private/aws_iot_taskpool_internal.h"

/* Linear containers include. */
#include "aws_iot_taskpool.h"

/* Test framework includes. */
#include "unity_fixture.h"

/*-----------------------------------------------------------*/

/**
* @brief A simple user context to prove all callbacks are called.
*/
typedef struct JobUserContext
{
    AwsIotMutex_t lock;    /**< @brief Protection from concurrent updates. */
    uint32_t      counter; /**< @brief A counter to keep track of callback invokations. */
} JobUserContext_t;

/*-----------------------------------------------------------*/

/**
 * @brief Test group for linear containers tests.
 */
TEST_GROUP( Common_Unit_TaskPool );

/*-----------------------------------------------------------*/

/**
 * @brief Test setup for linear containers tests.
 */
TEST_SETUP( Common_Unit_TaskPool )
{
}

/*-----------------------------------------------------------*/

/**
 * @brief Test tear down for linear containers.
 */
TEST_TEAR_DOWN( Common_Unit_TaskPool )
{
}

/*-----------------------------------------------------------*/

/**
 * @brief Test group runner for linear containers.
 */
TEST_GROUP_RUNNER( Common_Unit_TaskPool )
{
    RUN_TEST_CASE( Common_Unit_TaskPool, TaskPool_CreateDestroy );
    RUN_TEST_CASE( Common_Unit_TaskPool, TaskPool_CreateJobError );
    RUN_TEST_CASE( Common_Unit_TaskPool, TaskPool_ScheduleTasksError );
    RUN_TEST_CASE( Common_Unit_TaskPool, TaskPool_ScheduleTasks_ScheduleOneThenWait );
    RUN_TEST_CASE( Common_Unit_TaskPool, TaskPool_ScheduleTasks_ScheduleAllThenWait );
    RUN_TEST_CASE( Common_Unit_TaskPool, TaskPool_CancelTasks );
}

/*-----------------------------------------------------------*/

/**
* @brief Number of iterations for each test loop.
*/
#ifndef _TASKPOOL_TEST_ITERATIONS
#define _TASKPOOL_TEST_ITERATIONS ( 20 )
#endif

/**
* @brief Define the stress job max duration time (emulated duration).
*/
#ifndef _TASKPOOL_TEST_WORK_ITEM_DURATION_MAX
#define _TASKPOOL_TEST_WORK_ITEM_DURATION_MAX ( 55 )
#endif 

/**
* @brief A global delay to wait for threads to exit or such...
*/
struct itimerspec _TEST_DELAY_50MS =
{
    .it_value.tv_sec  = 0,
    .it_value.tv_nsec = ( 50000000L ), /* 50ms */
    .it_interval      = { 0 }
};

/* ---------------------------------------------------------- */

/**
* @brief A function that emulates some work in the task pool execution by sleeping.
*/
static void EmulateWork( )
{
    int32_t duration_in_nsec = ( 1000000 ) * ( rand( ) % _TASKPOOL_TEST_WORK_ITEM_DURATION_MAX );

    TEST_ASSERT_TRUE( duration_in_nsec <= 999999999 );

    struct timespec delay =
    {
        .tv_sec  = 0,
        .tv_nsec = duration_in_nsec
    };

    int error = clock_nanosleep( CLOCK_MONOTONIC, 0, &delay, NULL );

    TEST_ASSERT_TRUE( error == 0 );
}

/**
* @brief A function that blocks execution for .
*/
static void CleanupDelay( )
{
    struct timespec delay =
    {
        .tv_sec  = 1,
        .tv_nsec = 0
    };

    int error = clock_nanosleep( CLOCK_MONOTONIC, 0, &delay, NULL );

    TEST_ASSERT_TRUE( error == 0 );
}

/**
* @brief A callback that recycles its job.
*/
static void ExecutionWithDestroyCb( AwsIotTaskPool_t * pTaskPool, AwsIotTaskPoolJob_t * pJob, void * context )
{
    JobUserContext_t * pUserContext;
    AwsIotTaskPoolJobStatus_t status;

    TEST_ASSERT( AwsIotTaskPool_GetStatus( pJob, &status ) == AWS_IOT_TASKPOOL_SUCCESS );
    TEST_ASSERT( status == AWS_IOT_TASKPOOL_STATUS_EXECUTING );

    EmulateWork( );

    pUserContext = ( JobUserContext_t * )context;

    AwsIotMutex_Lock( &pUserContext->lock );
    pUserContext->counter++;
    AwsIotMutex_Unlock( &pUserContext->lock );

    TEST_ASSERT( AwsIotTaskPool_DestroyJob( pJob ) == AWS_IOT_TASKPOOL_SUCCESS );
}

/**
* @brief A callback that does not recycle its job.
*/
static void ExecutionWithoutDestroyCb( AwsIotTaskPool_t * pTaskPool, AwsIotTaskPoolJob_t * pJob, void * context )
{
    JobUserContext_t * pUserContext;
    AwsIotTaskPoolJobStatus_t status;

    TEST_ASSERT( AwsIotTaskPool_GetStatus( pJob, &status ) == AWS_IOT_TASKPOOL_SUCCESS );
    TEST_ASSERT( status == AWS_IOT_TASKPOOL_STATUS_EXECUTING );

    EmulateWork( );

    pUserContext = ( JobUserContext_t * )context;

    AwsIotMutex_Lock( &pUserContext->lock );
    pUserContext->counter++;
    AwsIotMutex_Unlock( &pUserContext->lock );
}

/* ---------------------------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */

/**
* @brief Number of legal task pool initialization configurations.
*/
#define LEGAL_INFOS 3

/**
* @brief Number of illegal task pool initialization configurations.
*/
#define ILLEGAL_INFOS 3

/**
* @brief Legal initialization configurations.
*/
AwsIotTaskPoolInfo_t tpInfoLegal[ LEGAL_INFOS ] = {
    { .minThreads = 1, .maxThreads = 1, .stackSize = AWS_IOT_TASKPOOL_THREADS_STACK_SIZE, .priority = AWS_IOT_TASKPOOL_THREADS_PRIORITY },
    { .minThreads = 1, .maxThreads = 2, .stackSize = AWS_IOT_TASKPOOL_THREADS_STACK_SIZE, .priority = AWS_IOT_TASKPOOL_THREADS_PRIORITY },
    { .minThreads = 2, .maxThreads = 3, .stackSize = AWS_IOT_TASKPOOL_THREADS_STACK_SIZE, .priority = AWS_IOT_TASKPOOL_THREADS_PRIORITY }
};

/**
* @brief Illegal initialization configurations.
*/
AwsIotTaskPoolInfo_t tpInfoIllegal[ ILLEGAL_INFOS ] = {
    { .minThreads = 0, .maxThreads = 1, .stackSize = AWS_IOT_TASKPOOL_THREADS_STACK_SIZE, .priority = AWS_IOT_TASKPOOL_THREADS_PRIORITY },
    { .minThreads = 1, .maxThreads = 0, .stackSize = AWS_IOT_TASKPOOL_THREADS_STACK_SIZE, .priority = AWS_IOT_TASKPOOL_THREADS_PRIORITY },
    { .minThreads = 2, .maxThreads = 1, .stackSize = AWS_IOT_TASKPOOL_THREADS_STACK_SIZE, .priority = AWS_IOT_TASKPOOL_THREADS_PRIORITY }
};

/*-----------------------------------------------------------*/

/**
* @brief Test task pool dynamic memory creation and destruction, with both legal and illegal information.
*/
TEST( Common_Unit_TaskPool, TaskPool_CreateDestroy )
{
    uint32_t count;
    AwsIotTaskPool_t taskPool;

    for ( count = 0; count < LEGAL_INFOS; ++count )
    {
        TEST_ASSERT( AwsIotTaskPool_Create( &tpInfoLegal[ count ], &taskPool ) == AWS_IOT_TASKPOOL_SUCCESS );
        TEST_ASSERT( AwsIotTaskPool_Destroy( &taskPool ) == AWS_IOT_TASKPOOL_SUCCESS );
    }

    for ( count = 0; count < ILLEGAL_INFOS; ++count )
    {
        TEST_ASSERT( AwsIotTaskPool_Create( &tpInfoIllegal[ count ], &taskPool ) == AWS_IOT_TASKPOOL_BAD_PARAMETER );
    }

    TEST_ASSERT( AwsIotTaskPool_Create( &tpInfoLegal[ 0 ], NULL ) == AWS_IOT_TASKPOOL_BAD_PARAMETER );
    TEST_ASSERT( AwsIotTaskPool_Create( NULL, &taskPool ) == AWS_IOT_TASKPOOL_BAD_PARAMETER );
    
    CleanupDelay( );
}

/*-----------------------------------------------------------*/

/**
* @brief Test task pool job static and dynamic memory creation with bogus parameters.
*/
TEST( Common_Unit_TaskPool, TaskPool_CreateJobError )
{
    AwsIotTaskPoolInfo_t tpInfo = { .minThreads = 2, .maxThreads = 3, .stackSize = AWS_IOT_TASKPOOL_THREADS_STACK_SIZE, .priority = AWS_IOT_TASKPOOL_THREADS_PRIORITY };

    {
        AwsIotTaskPoolJob_t job;

        /* NULL callback. */
        TEST_ASSERT( AwsIotTaskPool_CreateJob( NULL, NULL, &job ) == AWS_IOT_TASKPOOL_BAD_PARAMETER );
        /* NULL job handle. */
        TEST_ASSERT( AwsIotTaskPool_CreateJob( &ExecutionWithDestroyCb, NULL, NULL ) == AWS_IOT_TASKPOOL_BAD_PARAMETER );
    }

    CleanupDelay( );
}

/*-----------------------------------------------------------*/

/**
* @brief Test scheduling a job with bad parameters.
*/
TEST( Common_Unit_TaskPool, TaskPool_ScheduleTasksError )
{
    AwsIotTaskPool_t taskPool;
    AwsIotTaskPoolInfo_t tpInfo = { .minThreads = 2, .maxThreads = 3, .stackSize = AWS_IOT_TASKPOOL_THREADS_STACK_SIZE, .priority = AWS_IOT_TASKPOOL_THREADS_PRIORITY };

    AwsIotTaskPool_Create( &tpInfo, &taskPool );

    AwsIotTaskPoolJob_t job;

    TEST_ASSERT( AwsIotTaskPool_CreateJob( &ExecutionWithDestroyCb, NULL, &job ) == AWS_IOT_TASKPOOL_SUCCESS );

    /* NULL Task Pool Handle. */
    TEST_ASSERT( AwsIotTaskPool_Schedule( NULL, &job ) == AWS_IOT_TASKPOOL_BAD_PARAMETER );
    /* NULL Work item Handle. */
    TEST_ASSERT( AwsIotTaskPool_Schedule( &taskPool, NULL ) == AWS_IOT_TASKPOOL_BAD_PARAMETER );
    /* Destroy the job, so we do not leak it. */
    TEST_ASSERT( AwsIotTaskPool_DestroyJob( &job ) == AWS_IOT_TASKPOOL_SUCCESS );

    AwsIotTaskPool_Destroy( &taskPool );

    CleanupDelay( );
}

/*-----------------------------------------------------------*/

/**
* @brief Test scheduling a set of jobs: static allocation, sequential execution.
*/
TEST( Common_Unit_TaskPool, TaskPool_ScheduleTasks_ScheduleOneThenWait )
{
    AwsIotTaskPool_t taskPool;
    AwsIotTaskPoolInfo_t tpInfo = { .minThreads = 2, .maxThreads = 3, .stackSize = AWS_IOT_TASKPOOL_THREADS_STACK_SIZE, .priority = AWS_IOT_TASKPOOL_THREADS_PRIORITY };

    AwsIotTaskPool_Create( &tpInfo, &taskPool );

    /* Statically allocated job, schedule one, then wait. */
    {
        uint32_t count;
        uint32_t scheduled = 0;
        JobUserContext_t userContext = { 0 };
        AwsIotTaskPoolJob_t job;

        /* Initialize user context. */
        TEST_ASSERT( AwsIotMutex_Create( &userContext.lock ) );

        /* Shedule the job NOT to be recycle in the callback, since the buffer is statically allocated. */
        TEST_ASSERT( AwsIotTaskPool_CreateJob( &ExecutionWithoutDestroyCb, &userContext, &job ) == AWS_IOT_TASKPOOL_SUCCESS );

        for ( count = 0; count < _TASKPOOL_TEST_ITERATIONS; ++count )
        {
            AwsIotTaskPoolError_t errorSchedule = AwsIotTaskPool_Schedule( &taskPool, &job );

            switch ( errorSchedule )
            {
            case AWS_IOT_TASKPOOL_SUCCESS:
                ++scheduled;
                break;
            case AWS_IOT_TASKPOOL_BAD_PARAMETER:
            case AWS_IOT_TASKPOOL_ILLEGAL_OPERATION:
            case AWS_IOT_TASKPOOL_SHUTDOWN_IN_PROGRESS:
                TEST_ASSERT( false );
                break;
            default:
                TEST_ASSERT( false );
            }

            AwsIotTaskPoolError_t errorWait =  AwsIotTaskPool_Wait( &taskPool, &job );

            switch ( errorWait )
            {
            case AWS_IOT_TASKPOOL_SUCCESS:
                break;
            case AWS_IOT_TASKPOOL_BAD_PARAMETER:
            case AWS_IOT_TASKPOOL_SHUTDOWN_IN_PROGRESS:
            case AWS_IOT_TASKPOOL_TIMEDOUT:
                TEST_ASSERT( false );
                break;
            default:
                TEST_ASSERT( false );
            }

            /* Ensure callback actually executed. */
            TEST_ASSERT( userContext.counter == scheduled );
        }

        TEST_ASSERT( AwsIotTaskPool_DestroyJob( &job ) == AWS_IOT_TASKPOOL_SUCCESS );

        /* Since jobs were build from a static buffer and scheduled one-by-one, we
        * should have received all callbacks.
        */
        TEST_ASSERT( scheduled == _TASKPOOL_TEST_ITERATIONS );

        /* Destroy user context. */
        AwsIotMutex_Destroy( &userContext.lock );
    }

    AwsIotTaskPool_Destroy( &taskPool );

    CleanupDelay( );
}

/*-----------------------------------------------------------*/

/**
* @brief Test scheduling a set of jobs: static allocation, bulk execution.
*/
TEST( Common_Unit_TaskPool, TaskPool_ScheduleTasks_ScheduleAllThenWait )
{
    AwsIotTaskPool_t taskPool;
    AwsIotTaskPoolInfo_t tpInfo = { .minThreads = 2, .maxThreads = 3, .stackSize = AWS_IOT_TASKPOOL_THREADS_STACK_SIZE, .priority = AWS_IOT_TASKPOOL_THREADS_PRIORITY };

    AwsIotTaskPool_Create( &tpInfo, &taskPool );

    /* Statically allocated jobs, schedule all, then wait all. */
    {
        uint32_t count;
        uint32_t scheduled = 0;
        JobUserContext_t userContext = { 0 };
        AwsIotTaskPoolJob_t tpJobs[ _TASKPOOL_TEST_ITERATIONS ] = { 0 };

        /* Initialize user context. */
        TEST_ASSERT( AwsIotMutex_Create( &userContext.lock ) );

        for ( count = 0; count < _TASKPOOL_TEST_ITERATIONS; ++count )
        {
            /* Shedule the job NOT to be recycle in the callback, since the buffer is statically allocated. */
            TEST_ASSERT( AwsIotTaskPool_CreateJob( &ExecutionWithoutDestroyCb, &userContext, &tpJobs[ count ] ) == AWS_IOT_TASKPOOL_SUCCESS );

            AwsIotTaskPoolError_t errorSchedule = AwsIotTaskPool_Schedule( &taskPool, &tpJobs[ count ] );

            switch ( errorSchedule )
            {
            case AWS_IOT_TASKPOOL_SUCCESS:
                ++scheduled;
                break;
            case AWS_IOT_TASKPOOL_BAD_PARAMETER:
            case AWS_IOT_TASKPOOL_ILLEGAL_OPERATION:
                TEST_ASSERT( false );
                break;
            default:
                TEST_ASSERT( false );
            }
        }

        for ( count = 0; count < _TASKPOOL_TEST_ITERATIONS; ++count )
        {
            AwsIotTaskPoolError_t errorWait = AwsIotTaskPool_Wait( &taskPool, &tpJobs[ count ] );

            switch ( errorWait )
            {
            case AWS_IOT_TASKPOOL_SUCCESS:
                break;
            case AWS_IOT_TASKPOOL_BAD_PARAMETER:
            case AWS_IOT_TASKPOOL_SHUTDOWN_IN_PROGRESS:
            case AWS_IOT_TASKPOOL_TIMEDOUT:
                TEST_ASSERT( false );
                break;
            default:
                TEST_ASSERT( false );
            }
        }

        /* Wait until callback is executed. */
        TEST_ASSERT_TRUE( userContext.counter == scheduled );

        for ( count = 0; count < _TASKPOOL_TEST_ITERATIONS; ++count )
        {
            TEST_ASSERT( AwsIotTaskPool_DestroyJob( &tpJobs[ count ] ) == AWS_IOT_TASKPOOL_SUCCESS );
        }

        /* Destroy user context. */
        AwsIotMutex_Destroy( &userContext.lock );
    }
    
    AwsIotTaskPool_Destroy( &taskPool );

    CleanupDelay( );
}

/*-----------------------------------------------------------*/

/**
* @brief Test scheduling and canceling jobs.
*/
TEST( Common_Unit_TaskPool, TaskPool_CancelTasks )
{
    uint32_t count;
    AwsIotTaskPool_t taskPool;
    AwsIotTaskPoolInfo_t tpInfo = { .minThreads = 2, .maxThreads = 3, .stackSize = AWS_IOT_TASKPOOL_THREADS_STACK_SIZE, .priority = AWS_IOT_TASKPOOL_THREADS_PRIORITY };
    uint32_t canceled = 0;
    uint32_t scheduled = 0;

    AwsIotTaskPool_Create( &tpInfo, &taskPool );

    JobUserContext_t userContext = { 0 };
    AwsIotTaskPoolJob_t jobs[ _TASKPOOL_TEST_ITERATIONS ] = { 0 };

    /* Initialize user context. */
    TEST_ASSERT( AwsIotMutex_Create( &userContext.lock ) );

    /* Create and schedule loop. */
    for ( count = 0; count < _TASKPOOL_TEST_ITERATIONS; ++count )
    {
        AwsIotTaskPoolError_t errorMake = AwsIotTaskPool_CreateJob( &ExecutionWithoutDestroyCb, &userContext, &jobs[ count ] );

        switch ( errorMake )
        {
        case AWS_IOT_TASKPOOL_SUCCESS:
            break;
        case AWS_IOT_TASKPOOL_NO_MEMORY: /* OK. */
            continue;
        case AWS_IOT_TASKPOOL_BAD_PARAMETER:
            TEST_ASSERT( false );
            break;
        default:
            TEST_ASSERT( false );
        }

        AwsIotTaskPoolError_t errorSchedule = AwsIotTaskPool_Schedule( &taskPool, &jobs[ count ] );

        switch ( errorSchedule )
        {
        case AWS_IOT_TASKPOOL_SUCCESS:
            ++scheduled;
            break;
        case AWS_IOT_TASKPOOL_BAD_PARAMETER:
        case AWS_IOT_TASKPOOL_ILLEGAL_OPERATION:
        case AWS_IOT_TASKPOOL_SHUTDOWN_IN_PROGRESS:
            TEST_ASSERT( false );
            break;
        default:
            TEST_ASSERT( false );
        }
    }

    /* Cancellation loop. */
    for ( count = 0; count < _TASKPOOL_TEST_ITERATIONS; ++count )
    {
        AwsIotTaskPoolError_t error;
        AwsIotTaskPoolJobStatus_t statusAtCancellation = AWS_IOT_TASKPOOL_STATUS_READY;
        AwsIotTaskPoolJobStatus_t statusAfterCancellation = AWS_IOT_TASKPOOL_STATUS_READY;
        
        error = AwsIotTaskPool_TryCancel( &taskPool, &jobs[ count ], &statusAtCancellation );

        switch ( error )
        {
        case AWS_IOT_TASKPOOL_SUCCESS:
        {
            canceled++;
            TEST_ASSERT( ( statusAtCancellation == AWS_IOT_TASKPOOL_STATUS_READY ) || ( statusAtCancellation == AWS_IOT_TASKPOOL_STATUS_SCHEDULED ) || ( statusAtCancellation == AWS_IOT_TASKPOOL_STATUS_CANCELED ) );

            TEST_ASSERT( AwsIotTaskPool_GetStatus( &jobs[ count ], &statusAfterCancellation ) == AWS_IOT_TASKPOOL_SUCCESS );
            TEST_ASSERT( statusAfterCancellation == AWS_IOT_TASKPOOL_STATUS_CANCELED );
        }
        break;
        case AWS_IOT_TASKPOOL_CANCEL_FAILED:
        {
            TEST_ASSERT( ( statusAtCancellation == AWS_IOT_TASKPOOL_STATUS_EXECUTING ) || ( statusAtCancellation == AWS_IOT_TASKPOOL_STATUS_COMPLETED ) );

            TEST_ASSERT( AwsIotTaskPool_GetStatus( &jobs[ count ], &statusAfterCancellation ) == AWS_IOT_TASKPOOL_SUCCESS );
            TEST_ASSERT( ( statusAfterCancellation == AWS_IOT_TASKPOOL_STATUS_EXECUTING ) || ( statusAfterCancellation == AWS_IOT_TASKPOOL_STATUS_COMPLETED ) );
        }
        break;
        case AWS_IOT_TASKPOOL_SHUTDOWN_IN_PROGRESS:
            /* This must be a test issue. */
            TEST_ASSERT( false );
            break;
        default:
            TEST_ASSERT( false );
            break;
        }
    }

    /* Wait until callback is executed. */
    while ( ( scheduled - canceled ) != userContext.counter )
    {
        ( void )clock_nanosleep( CLOCK_REALTIME, 0, &_TEST_DELAY_50MS.it_value, NULL );
    }

    TEST_ASSERT( ( scheduled - canceled ) == userContext.counter );

    for ( count = 0; count < _TASKPOOL_TEST_ITERATIONS; ++count )
    {
        TEST_ASSERT( AwsIotTaskPool_DestroyJob( &jobs[ count ] ) == AWS_IOT_TASKPOOL_SUCCESS );
    }

    /* Destroy user context. */
    AwsIotMutex_Destroy( &userContext.lock );

    AwsIotTaskPool_Destroy( &taskPool );

    CleanupDelay( );
}

/*-----------------------------------------------------------*/
