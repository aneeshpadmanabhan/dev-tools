#!/bin/sh


echo "Deploying Display component: $now "

mkdir -p /opt/oneops

mv $OO_HOME/dist/oneops/dist/app.tar.gz /opt/oneops/app.tar.gz

cd /opt/oneops

# backup current app director if it exists
if [ -d app ]
  rm -fr app~
  mv app app~
fi

tar -xzvf app.tar.gz

bundle install

dbversion=$(rake db:version | grep "Current version:" | awk '{print $3}')
echo "Current db version" $dbversion
if [ "$dbversion" ==  "0" ]; then
   rake db:setup
else
   rake db:migrate
fi

now=$(date +"%T")

cp $OO_HOME/start-display.sh /opt/oneops

chmod +x /opt/oneops/start-display.sh

echo "Done with Display: $now "
