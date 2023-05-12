consul version
# Consul v1.15.0
consul agent -dev -node machine

visit: http://127.0.0.1:8500

consul catalog services

http://127.0.0.1:8500/ui/dc1/services
