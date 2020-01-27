FROM node:lts

ENV SRV_DIR /srv/grpc

RUN mkdir -p "${SRV_DIR}"

COPY ./* "${SRV_DIR}/"

RUN cd "${SRV_DIR}" && npm install

CMD ["node", "/srv/grpc/greeter_server.js"]