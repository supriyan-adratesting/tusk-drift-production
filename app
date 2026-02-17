server {
    listen 80;
    location / {
        include uwsgi_params;
        uwsgi_pass unix:/home/second_careers_project/app.sock;
        uwsgi_read_timeout 1800;
        uwsgi_ignore_client_abort on;
        client_max_body_size 1024M;
    }
    server_name devapi.2ndcareers.com;
    error_log /dev/stderr;
    access_log /dev/stdout;
}
