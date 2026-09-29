<?php

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use Temporal\Activity\ActivityInterface;
use Temporal\Activity\ActivityMethod;
use Temporal\Activity\ActivityOptions;
use Temporal\Common\RetryOptions;
use Temporal\Worker\WorkerOptions;
use Temporal\Workflow;
use Temporal\Workflow\WorkflowInterface;
use Temporal\Workflow\WorkflowMethod;

#[ActivityInterface(prefix: "SlowActivity.")]
class SlowActivity
{
    #[ActivityMethod]
    public function sleep(int $seconds): string
    {
        \sleep($seconds);

        return 'done';
    }
}

#[WorkflowInterface]
class SlowActivityWorkflow
{
    #[WorkflowMethod(name: 'SlowActivityWorkflow')]
    public function handler(int $seconds)
    {
        return yield Workflow::newActivityStub(
            SlowActivity::class,
            ActivityOptions::new()
                ->withStartToCloseTimeout('30 seconds')
                ->withRetryOptions(RetryOptions::new()->withMaximumAttempts(1)),
        )->sleep($seconds);
    }
}

$factory = Temporal\WorkerFactory::create();

$factory->newWorker('default', WorkerOptions::new()->withWorkerStopTimeout(10))
    ->registerWorkflowTypes(SlowActivityWorkflow::class)
    ->registerActivityImplementations(new SlowActivity());

$factory->run();
