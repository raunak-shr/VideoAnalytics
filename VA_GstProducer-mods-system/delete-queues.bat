@echo off
for /f "tokens=1" %%q in ('rabbitmqctl list_queues name') do (
    rabbitmqctl delete_queue %%q
)