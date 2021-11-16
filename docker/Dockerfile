FROM debian:bullseye-slim
COPY . .
RUN chmod +x arpcnet

EXPOSE 8028/TCP
EXPOSE 8029/TCP

ENTRYPOINT ["./arpcnet"]
