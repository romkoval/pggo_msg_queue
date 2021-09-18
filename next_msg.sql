with next_msg as (
        select msg_id from msg_queue mq
        where
            queue_type = $1
            and status = 'N'
            and not exists (select 1 from msg_queue
                            where
                            queue_type=mq.queue_type
                            and order_id = mq.order_id
                            and status='N'
                            and msg_id != mq.msg_id
                            and time_added <= mq.time_added)
        limit 1
        for update skip locked
    )
    update msg_queue mq
        set status='P'
        from next_msg
        where mq.msg_id = next_msg.msg_id
    returning mq.msg_id, mq.queue_type, mq.order_id
