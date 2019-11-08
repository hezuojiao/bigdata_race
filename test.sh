cd build
make clean
make
cd ..
./build/bigdata_race /data/customer.txt /data/orders.txt /data/lineitem.txt 1 BUILDING 1998-08-02 1992-01-02 5
./build/bigdata_race /data/customer.txt /data/orders.txt /data/lineitem.txt 3 BUILDING 1995-03-29 1995-03-27 5 BUILDING 1995-02-29 1995-04-27 10 BUILDING 1995-03-28 1995-04-27 2
