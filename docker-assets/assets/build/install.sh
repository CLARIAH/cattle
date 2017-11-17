#!/bin/bash
set -e


## Execute a command as GITLAB_USER
exec_as_cattle() {
  if [[ $(whoami) == ${CATTLE_USER} ]]; then
    $@
  else
    sudo -HEu ${CATTLE_USER} "$@"
  fi
}

#add cattle user
adduser --disabled-login --gecos 'cattle' ${CATTLE_USER}
passwd -d ${CATTLE_USER}


cd ${CATTLE_INSTALL_DIR}
chown ${CATTLE_USER}:${CATTLE_USER} ${CATTLE_HOME} -R

pip install -r src/requirements.txt
sudo pip install cow_csvw

#move nginx logs to ${GITLAB_LOG_DIR}/nginx
sed -i \
 -e "s|access_log /var/log/nginx/access.log;|access_log ${CATTLE_LOG_DIR}/nginx/access.log;|" \
 -e "s|error_log /var/log/nginx/error.log;|error_log ${CATTLE_LOG_DIR}/nginx/error.log;|" \
 /etc/nginx/nginx.conf


 # configure gitlab log rotation
 cat > /etc/logrotate.d/grlc << EOF
 ${CATTLE_LOG_DIR}/cattle/*.log {
   weekly
   missingok
   rotate 52
   compress
   delaycompress
   notifempty
   copytruncate
 }
 EOF

 # configure gitlab vhost log rotation
 cat > /etc/logrotate.d/grlc-nginx << EOF
 ${CATTLE_LOG_DIR}/nginx/*.log {
   weekly
   missingok
   rotate 52
   compress
   delaycompress
   notifempty
   copytruncate
 }
 EOF
