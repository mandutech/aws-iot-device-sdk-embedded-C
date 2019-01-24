/*
 * Copyright (C) 2018 Amazon.com, Inc. or its affiliates.  All Rights Reserved.
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
 * @file iot_tests_taskpool_stress.c
 * @brief Stress tests for the AWS IoT Task Pool library.
 *
 * The tests in this file run far longer than other tests, and may easily fail
 * due to poor network conditions. For best results, these tests should be run
 * on a stable local network (not the Internet).
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

 /**
 * @brief Define the outer loop stress iterations.
 */
#ifndef _TASKPOOL_STRESS_ITERATIONS_OUTER
#define _TASKPOOL_STRESS_ITERATIONS_OUTER ( 10 )
#endif

 /**
 * @brief Define the inner loop stress iterations.
 */
#ifndef _TASKPOOL_STRESS_ITERATIONS_INNER
#define _TASKPOOL_STRESS_ITERATIONS_INNER ( 100 )
#endif

 /**
 * @brief Define the stress job max duration time (emulated duration).
 */
#ifndef _TASKPOOL_TEST_WORK_ITEM_DURATION_MAX
#define _TASKPOOL_TEST_WORK_ITEM_DURATION_MAX (55 )
#endif 

 /**
 * @brief Define the stress job max duration time (emulated duration).
 */
#ifndef _TASKPOOL_TEST_WAIT_TIME
#define _TASKPOOL_TEST_WAIT_TIME     ( 60 * 60 * 1000 ) /* One hour in milli-seconds. */
#endif

 /**
 * @brief Active jobs created by the stress application.
 */
IotLink_t activeRequests = { 0 };

/**
* @brief The Tasm Pool test group.
*/
TEST_GROUP( Common_Stress_TaskPool );

/*-----------------------------------------------------------*/

/**
* @brief Setup function for this unit test.
*/
TEST_SETUP( Common_Stress_TaskPool )
{
    /* Seed the randon number generator. */
    srand ( ( int )time( NULL ) );

    IotQueue_Create( &activeRequests );
}

/**
* @brief A job created by the stress application.
*/
typedef struct StressJob
{
    IotLink_t activeRequestsLink;                          /**< @brief Link to the queue of active requests. */
    AwsIotTaskPoolJob_t * pJob;    /**< @brief The handle for this job. */
} StressJob_t;

/*-----------------------------------------------------------*/

/**
* @brief Tear down function for this unit test.
*/
TEST_TEAR_DOWN( Common_Stress_TaskPool )
{ }

/*-----------------------------------------------------------*/

/**
* @brief The group of tests to run.
*/
TEST_GROUP_RUNNER( Common_Stress_TaskPool )
{
    RUN_TEST_CASE( Common_Stress_TaskPool, SingleWorkItems );
    RUN_TEST_CASE( Common_Stress_TaskPool, SingleWorkItemsPlusCancellation );
    RUN_TEST_CASE( Common_Stress_TaskPool, MultipleWorkItems );
    RUN_TEST_CASE( Common_Stress_TaskPool, MultipleWorkItemsPlusCancellation );
}

/* ---------------------------------------------------------------------------------------------- */

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
static void ExecutionWithRecycleCb( AwsIotTaskPool_t * pTaskPool, AwsIotTaskPoolJob_t * pJob, void * context )
{
    AwsIotTaskPoolJobStatus_t status;

    TEST_ASSERT( AwsIotTaskPool_GetStatus( pJob, &status ) == AWS_IOT_TASKPOOL_SUCCESS );
    TEST_ASSERT( status == AWS_IOT_TASKPOOL_STATUS_EXECUTING );

    EmulateWork( );

    TEST_ASSERT( AwsIotTaskPool_RecycleJob( pTaskPool, pJob ) == AWS_IOT_TASKPOOL_SUCCESS );
}

/**
* @brief A callback that does not recycle its job.
*/
static void ExecutionWithoutRecycleCb( AwsIotTaskPool_t * pTaskPool, AwsIotTaskPoolJob_t * pJob, void * context )
{
    AwsIotTaskPoolJobStatus_t status;

    TEST_ASSERT( AwsIotTaskPool_GetStatus( pJob, &status ) == AWS_IOT_TASKPOOL_SUCCESS );
    TEST_ASSERT( status == AWS_IOT_TASKPOOL_STATUS_EXECUTING );

    EmulateWork( );
}

/* ---------------------------------------------------------------------------------------------- */

/**
* @brief A function to create a job.
*/
StressJob_t * CreateWorkItem__Single( AwsIotTaskPool_t * pTaskPool )
{
    AwsIotTaskPoolJob_t * pJob;

    AwsIotTaskPoolError_t error = AwsIotTaskPool_CreateJob( pTaskPool, &ExecutionWithRecycleCb, NULL, &pJob );

    if ( error  == AWS_IOT_TASKPOOL_SUCCESS )
    {
        StressJob_t * pStress = malloc( sizeof( StressJob_t ) );

        if ( pStress != NULL )
        {
            pStress->activeRequestsLink.pNext = NULL;
            pStress->activeRequestsLink.pPrevious = NULL;

            pStress->pJob = pJob;

            return pStress;
        }
        else
        {
            TEST_ASSERT( AwsIotTaskPool_RecycleJob( pTaskPool, pJob ) == AWS_IOT_TASKPOOL_SUCCESS );
        }
    }

    return NULL;
}

/* ---------------------------------------------------------------------------------------------- */

/**
* @brief Test scheduling a flurry of single shot jobs, without waiting for completion.
*/
TEST( Common_Stress_TaskPool, SingleWorkItems )
{
    uint32_t countInner, countOuter;
    AwsIotTaskPool_t * pTaskPool;
    AwsIotTaskPoolInfo_t tpInfo = { .minThreads = 2, .maxThreads = 5, .stackSize = AWS_IOT_TASKPOOL_THREADS_STACK_SIZE, .priority = AWS_IOT_TASKPOOL_THREADS_PRIORITY };

    TEST_ASSERT( AwsIotTaskPool_Create( &tpInfo, &pTaskPool ) == AWS_IOT_TASKPOOL_SUCCESS );
    TEST_ASSERT( pTaskPool != NULL );

    countOuter = 0;
    for ( ; countOuter < _TASKPOOL_STRESS_ITERATIONS_OUTER; ++countOuter )
    {
        IotQueue_Create( &activeRequests );

        /* Schedule jobs. */
        countInner = 0;
        for ( ; countInner < _TASKPOOL_STRESS_ITERATIONS_INNER; ++countInner )
        {
            StressJob_t * pJob = CreateWorkItem__Single( pTaskPool );

            if ( pJob != NULL )
            {
                AwsIotTaskPoolError_t error = AwsIotTaskPool_Schedule( pTaskPool, pJob->pJob );

                if ( error == AWS_IOT_TASKPOOL_SUCCESS )
                {
                    IotQueue_Enqueue( &activeRequests, &pJob->activeRequestsLink );
                }
                else
                {
                    TEST_ASSERT( AwsIotTaskPool_RecycleJob( pTaskPool, pJob->pJob ) == AWS_IOT_TASKPOOL_SUCCESS );

                    free( pJob );
                }
            }
        }

        /* Infinite wait. */
        {
            IotLink_t * pLink;
            IotContainers_ForEach( &activeRequests, pLink )
            {
                StressJob_t * pItem = IotLink_Container( StressJob_t, pLink, activeRequestsLink );

                TEST_ASSERT( AwsIotTaskPool_Wait( pTaskPool, pItem->pJob ) == AWS_IOT_TASKPOOL_SUCCESS );
            }
        }

        {
            IotLink_t * pLink, * pTempLink;
            IotContainers_ForEachSafe( &activeRequests, pLink, pTempLink )
            {
                StressJob_t * pItem = IotLink_Container( StressJob_t, pLink, activeRequestsLink );

                free( pItem );
            }
        }
    }

    TEST_ASSERT( AwsIotTaskPool_Destroy( pTaskPool ) == AWS_IOT_TASKPOOL_SUCCESS );

    CleanupDelay( );
}

/* ---------------------------------------------------------- */

/**
* @brief Test scheduling a flurry of single shot jobs, including cancellation.
*/
TEST( Common_Stress_TaskPool, SingleWorkItemsPlusCancellation )
{
    uint32_t countInner, countOuter;
    AwsIotTaskPool_t * pTaskPool;
    AwsIotTaskPoolInfo_t tpInfo = { .minThreads = 2, .maxThreads = 5, .stackSize = AWS_IOT_TASKPOOL_THREADS_STACK_SIZE, .priority = AWS_IOT_TASKPOOL_THREADS_PRIORITY };

    TEST_ASSERT( AwsIotTaskPool_Create( &tpInfo, &pTaskPool ) == AWS_IOT_TASKPOOL_SUCCESS );
    TEST_ASSERT( pTaskPool != NULL );

    countOuter = 0;
    for ( ; countOuter < _TASKPOOL_STRESS_ITERATIONS_OUTER; ++countOuter )
    {
        IotQueue_Create( &activeRequests );

        /* Schedule jobs. */
        countInner = 0;
        for ( ; countInner < _TASKPOOL_STRESS_ITERATIONS_INNER; ++countInner )
        {
            StressJob_t * pJob = CreateWorkItem__Single( pTaskPool );

            if ( pJob != NULL )
            {
                AwsIotTaskPoolError_t error = AwsIotTaskPool_Schedule( pTaskPool, pJob->pJob );

                if ( error == AWS_IOT_TASKPOOL_SUCCESS )
                {
                    IotQueue_Enqueue( &activeRequests, &pJob->activeRequestsLink );
                }
                else
                {
                    TEST_ASSERT( AwsIotTaskPool_RecycleJob( pTaskPool, pJob->pJob ) == AWS_IOT_TASKPOOL_SUCCESS );

                    free( pJob );
                }
            }
        }

        /* Cancel some. */
        {
            IotLink_t * pLink;
            IotContainers_ForEach( &activeRequests, pLink )
            {
                AwsIotTaskPoolJobStatus_t status;

                if ( ( rand( ) % 2 ) == 0 )
                {
                    continue;
                }

                StressJob_t * pItem = IotLink_Container( StressJob_t, pLink, activeRequestsLink );

                if ( AwsIotTaskPool_TryCancel( pTaskPool, pItem->pJob, &status ) == AWS_IOT_TASKPOOL_SUCCESS )
                {
                    TEST_ASSERT( ( status == AWS_IOT_TASKPOOL_STATUS_READY ) || ( status == AWS_IOT_TASKPOOL_STATUS_SCHEDULED ) || ( status == AWS_IOT_TASKPOOL_STATUS_CANCELED ) );
                }
            }

            /* Infinite wait on the jobs that were not canceled. */
            {
                IotLink_t * pLink;
                IotContainers_ForEach( &activeRequests, pLink )
                {
                    AwsIotTaskPoolJobStatus_t status;

                    StressJob_t * pItem = IotLink_Container( StressJob_t, pLink, activeRequestsLink );

                    TEST_ASSERT( AwsIotTaskPool_GetStatus( pItem->pJob, &status ) == AWS_IOT_TASKPOOL_SUCCESS );

                    if ( status == AWS_IOT_TASKPOOL_STATUS_CANCELED )
                    {
                        continue;
                    }

                    TEST_ASSERT( AwsIotTaskPool_Wait( pTaskPool, pItem->pJob ) == AWS_IOT_TASKPOOL_SUCCESS );
                }
            }

            {
                IotLink_t * pLink, * pTempLink;
                IotContainers_ForEachSafe( &activeRequests, pLink, pTempLink )
                {
                    StressJob_t * pItem = IotLink_Container( StressJob_t, pLink, activeRequestsLink );

                    free( pItem );
                }
            }
        }
    }
    
    TEST_ASSERT( AwsIotTaskPool_Destroy( pTaskPool ) == AWS_IOT_TASKPOOL_SUCCESS );

    CleanupDelay( );
}

/* ---------------------------------------------------------- */

/**
* @brief Test scheduling a flurry of chained jobs, including waiting for completion and/or cancellation.
*/
TEST( Common_Stress_TaskPool, MultipleWorkItems )
{
    TEST_ASSERT( 1 );
}

/* ---------------------------------------------------------- */

/**
* @brief Test scheduling a flurry of chained jobs, including cancellation.
*/
TEST( Common_Stress_TaskPool, MultipleWorkItemsPlusCancellation )
{
    TEST_ASSERT( 1 );
}