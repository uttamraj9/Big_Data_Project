FROM nginx:1.25-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
# Remove default server block
RUN rm /etc/nginx/conf.d/default.conf.bak 2>/dev/null || true
EXPOSE 8088 9870 18088 7180 8889
HEALTHCHECK --interval=30s --timeout=3s CMD wget -qO- http://localhost:8088/ || exit 1
