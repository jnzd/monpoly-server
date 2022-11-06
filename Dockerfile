FROM monpoly-server:dev

# COPY ./src /backend

RUN apk add --no-cache python3 py3-pip \
    && pip3 install --upgrade pip \
    # && python3 -m .venv-monpoly \
    # && source .venv-monpoly/bin/activate \
    && pip3 install questdb psycopg2-binary flask

ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=5000

WORKDIR /monpoly-backend
# CMD [ "flask", "run" ]
ENTRYPOINT [ "sh" ]
