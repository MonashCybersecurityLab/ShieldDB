ShieldDB, which is an encrypted streaming database system, consists of both front-end and back-end components.

The system is developed by using Python 3.6. It requires Pycrypto 2.6.1 package, NLTK 3.3, Flask micro web framework, and RocksDB v.0.7.0 database.

NLTK, a textual language processing framework, can be found at https://www.nltk.org/install.html
Flask installation can be found at http://flask.pocoo.org/.

RocksDB installation can be found at https://github.com/twmht/python-rocksdb.

Here, we provide an example of streaming dataset and padding dataset regarding *alpha=156*. The data can be downloaded from this link.

https://drive.google.com/drive/folders/1e837hYzcwtxMn-IuEPFL8Uid8-ZdgqzA?usp=sharing

* The streaming dataset needs to be unzipped to a folder named as *streaming* and placed in the same directory of the system.
* The padding dataset should be unzipped to a folder named as *data256* and placed in the same directory of the system.


Deployment instruction:

1. Open a terminal and deploy Shield_server by using *python3 shield_server.py*

2. Open a terminal to deploy Shield_cli_streaming by using *python3 shield_stream.py*

3. After the *streaming* completed, open a terminal to deploy Shield_search by using *python3 shield_search.py*


Please contact us for technical issues at:
  + Viet Vo viet.vo@monash.edu
  + Xingliang Yuan xingliang.yuan@monash.edu
  + Shifeng Sun shifeng.sun@monash.edu
