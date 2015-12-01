package com.zyx.monitor;

/**
 * Created by yousheng on 15/6/15.
 */
public interface Monitor {

    void data_ready();

    void success_http();

    void mq_send();

    void mq_receive();

    void failed_mq(int num);

    void es_forwarded();

    void failed_es(int num);

    void data_error();

    void es_data_error();

    void es_data_saved(int num);

    void es_data_ready();
}
