From python:3.10
ENV PYTHONUNBUFFERED 1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on

WORKDIR /smapdir
COPY requirements.txt /smapdir/
RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt
