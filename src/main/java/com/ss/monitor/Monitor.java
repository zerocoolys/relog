package com.ss.monitor;

/**
 * Created by yousheng on 15/6/15.
 */
public interface Monitor {

    public void data_ready();

    public void success_http();

    public void mq_send();

    public void mq_receive();

    public void failed_mq(int num);

    public void es_forwarded();

    public void failed_es(int num);

    void data_error();

    void es_data_error();

    void es_data_saved(int num);

    void es_data_ready();
}
