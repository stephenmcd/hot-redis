.. image:: https://secure.travis-ci.org/stephenmcd/hot-redis.png?branch=master
   :target: http://travis-ci.org/stephenmcd/hot-redis

Created by `Stephen McDonald <http://twitter.com/stephen_mcd>`_

Introduction
============

HOT Redis is a wrapper library for the `redis-py`_ client. Rather than
calling the `Redis`_ commands directly from a client library, HOT Redis
provides a wide range of data types that mimic many of the built-in
data types provided by Python, such as lists, dicts, sets, and more, as
well as many of the classes found throughout the standard library, such
as those found in the Queue, threading, and collections modules.

These types are then backed by Redis, allowing objects to be
manipulated atomically over the network - the atomic nature of the
methods implemented on objects in HOT Redis is one of its core
features, and many of these are backed by `Lua`_ code executed within
Redis, which ensures atomic operations where applicable.

The name HOT Redis originally stood for "Higher Order Types for Redis",
but since the implementation doesn't strictly fit the definition, the
recursive acronym "HOT Object Toolkit for Redis" should appease the
most luscious of bearded necks.

HOT Redis was drawn from the infrastructure behind the
`Kouio RSS reader`_, a popular alternative to Google Reader.


Installation
============

The easiest way to install ``hot-redis`` is directly
from PyPi using `pip`_ by running the following command::

    $ pip install -U hot-redis

Otherwise you can download and install it directly from source::

    $ python setup.py install


Usage
=====

Each of the types provided by HOT Redis strive to implement the same
method signatures and return values as their Python built-in and
standard library counterparts. The main difference is each type's
``__init__`` method. Every HOT Redis type's ``__init__`` method will
optionally accept ``initial`` and ``key`` keyword arguments, which are
used for defining an initial value to be stored in Redis for the
object, and the key that should be used, respectively. If no key is
provided, a key will be generated, which can then be accessed via the
``key`` attribute::

    >>> from hot_redis import List
    >>> my_list = List()
    >>> my_list.key
    '93366bdb-90b2-4226-a52a-556f678af40e'
    >>> my_list_with_key = List(key="foo")
    >>> my_list_with_key.key
    'foo'

Once you've determined a strategy for naming keys, you can then create
HOT Redis objects and interact with them over the network, for example
here is a ``List`` created on a computer we'll refer to as computer A::

    >>> list_on_computer_a = List(key="foo", initial=["a", "b", "c"])

then on another computer we'll creatively refer to as computer B::

    >>> list_on_computer_b = List(key="foo")
    >>> list_on_computer_b[:]  # Performs: LRANGE foo 0 -1
    ['a', 'b', 'c']
    >>> list_on_computer_b += ['d', 'e', 'f']  # Performs: RPUSH foo d e f

and back to computer A::

    >>> list_on_computer_a[:]  # Performs: LRANGE foo 0 -1
    ['a', 'b', 'c', 'd', 'e', 'f']
    >>> 'c' in list_on_computer_a  # Works like Python lists where expected
    True
    >>> list_on_computer_a.reverse()
    >>> list_on_computer_a[:]
    ['f', 'e', 'd', 'c', 'b', 'a']

The last interaction here is an interesting one. Python's
``list.reverse()`` is an in-place reversal of the list, that is, it
modifies the existing list, rather than returning a reversed copy. If
we were to implement this naively, we would first read the list from
Redis, reverse it locally, then store the reversed list back in Redis
again. But what if another client were to modify the list at
approximately the same time? One computer's modification to the list
would certainly overwrite the other's. In this scenario, and *many*
others, HOT Redis provides its own Lua routine specifically for
reversing the list in-place, within Redis atomically. I wrote in more
detail about this in a blog post, `Bitwise Lua Operations in Redis`_.


Configuration
=============

By default, HOT Redis attempts to connect to a Redis instance running
locally on the default port 6379. You can configure the default client
by calling the ``hot_redis.configure`` function, prior to instantiating
any HOT Redis objects. The arguments given to ``configure`` are passed
onto the underlying `redis-py`_ client::

    >>> from hot_redis import configure
    configure(host='myremotehost', port=6380)

Alternatively, if you wish to use a different client per object, you
can explicitly create a ``HotClient`` instance, and pass it to each
object::

    >>> from hot_redis import HotClient, Queue
    >>> client = HotClient(host="myremotehost", port=6380)
    >>> my_queue = Queue(client=client)


Transactions
============

Basic support for thread-safe transactions are provided using the
Redis ``MULTI`` and ``EXEC`` commands::

    >>> from hot_redis import List, Queue, transaction
    >>> my_list = List(key="foo")
    >>> my_queue = Queue(key="bar")
    >>> with transaction():
    ...     for i in range(20):
    ...         my_list.append(i)
    ...         my_queue.put(i)

In the above example, all of the ``append`` and ``put`` calls are
batched together into a single transaction, that is executed once the
``transaction()`` context is exited.


Data Types
==========

The following table is the complete list of types provided by HOT
Redis, mapped to their Python counterparts and underlying Redis types,
along with any special considerations worth noting.

==================  ============================  ==========  ===============
HOT Redis           Python                        Redis       Notes
==================  ============================  ==========  ===============
List                list                          list
Set                 set                           set
Dict                dict                          hash
String              string                        string      Mutable - string methods that normally create a new string object in Python will mutate the string stored in Redis
ImmutableString     string                        string      Immutable - behaves like a regular Python string
Int                 int                           int
Float               float                         float
Queue               Queue.Queue                   list
LifoQueue           Queue.LifoQueue               list
SetQueue            N/A                           list + set  Extension of ``Queue`` with unique members
LifoSetQueue        N/A                           list + set  Extension of ``LifoQueue`` with unique members
BoundedSemaphore    threading.BoundedSemaphore    list        Extension of ``Queue`` leveraging Redis' blocking list pop operations with timeouts, while using Queue's ``maxsize`` arg to provide BoundedSemaphore's ``value`` arg
Semaphore           threading.Semaphore           list        Extension of ``BoundedSemaphore`` without a queue size
Lock                threading.Lock                list        Extension of ``BoundedSemaphore`` with a queue size of 1
RLock               threading.RLock               list        Extension of ``Lock`` allowing multiple ``acquire`` calls
DefaultDict         collections.DefaultDict       hash
MultiSet            collections.Counter           hash
==================  ============================  ==========  ===============

.. _`redis-py`: https://github.com/andymccurdy/redis-py
.. _`Redis`: http://redis.io
.. _`Lua`: http://www.lua.org/
.. _`Kouio RSS reader`: https://kouio.com
.. _`pip`: http://www.pip-installer.org/
.. _`Bitwise Lua Operations in Redis`: http://blog.jupo.org/2013/06/12/bitwise-lua-operations-in-redis/
