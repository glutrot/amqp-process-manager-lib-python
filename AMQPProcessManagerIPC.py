#!/bin/env python2.7

"""Module for communicating with AMQP Process Manager.
   See class AMQPProcessManagerIPC for details."""

__version__ = '0.1'
__copyright__ = 'Copyright 2015, glutrot GmbH'
__maintainer__ = 'Daniel Neugebauer'
__email__ = 'dneuge@glutrot.de'
__status__ = 'Production'

import json
import sys
import threading

class AMQPProcessManagerIPC:
    """Statically accessed class containing methods for inter-process
       communication with AMQP Process Manager. Call init() before use.
       Communication happens via stdin and stdout. To avoid colliding
       with other output, stdout is being redirected to stderr upon
       calling init(), so only call init() if you are really going to
       communicate via IPC.

       This class also contains a basic example for testing
       communication; please see source code for details."""
    
    from_lock = threading.Lock()
    to_lock = threading.Lock()
    
    fh_from_manager = None
    fh_to_manager = None
    
    @staticmethod
    def init():
        """Initializes IPC communication. Call once before using any other
           method of this class."""
        # redirect original stdout to stderr as stdout and stdin are used for IPC
        sys.stdout = sys.stderr
        
        # open stdin and stdout
        AMQPProcessManagerIPC.fh_from_manager = sys.__stdin__
        AMQPProcessManagerIPC.fh_to_manager = sys.__stdout__
    
    @staticmethod
    def _receive_ipc_message():
        """Receives an internal IPC message object from manager. Blocks."""
        AMQPProcessManagerIPC.from_lock.acquire(True)
        msg = AMQPProcessManagerIPC.fh_from_manager.readline()
        AMQPProcessManagerIPC.from_lock.release()
        
        msg_obj = json.loads(msg.rstrip())
        
        assert(len(msg_obj) >= 1)
        
        keyword = msg_obj[0]
        payload = msg_obj[1:]
        
        assert(keyword.strip() != '')
        
        return (keyword, payload)

    @staticmethod
    def _send_ipc_message(msg_obj):
        """Sends an internal IPC message object to manager."""
        s = json.dumps(msg_obj)
        
        AMQPProcessManagerIPC.to_lock.acquire(True)
        
        AMQPProcessManagerIPC.fh_to_manager.write(s)
        AMQPProcessManagerIPC.fh_to_manager.write('\n')
        AMQPProcessManagerIPC.fh_to_manager.flush()
        
        AMQPProcessManagerIPC.to_lock.release()

    @staticmethod
    def receive_message(decode=None):
        """Receives a message from manager. Will return a plain string if used
           without any decode method. Only supported decode method so far is
           'json'.
           Note that calling this method will block until a message could be
           received."""
        # check if decoder is known before blocking, map to function
        decoder = None
        if decode == 'json':
            decoder = json.loads
        elif not decode is None:
            raise Exception('No such decoder: %s' % decode)
        
        # wait for an IPC message of keyword "message"
        # NOTE: as we currently only have "message" keyword defined for manager => process, we just ignore everything else
        while True:
            msg_obj = AMQPProcessManagerIPC._receive_ipc_message()
            (keyword, data) = msg_obj

            if keyword == 'message':
                assert(len(data) == 1)
                break
        
        msg_content = data[0]
        
        # call decoder
        if not decoder is None:
            msg_content = decoder(msg_content)

        return msg_content
    
    @staticmethod
    def send_result(result, encode=None):
        """Submits the given result to manager. If used without any encode method,
           result should be a plain string. Only supported encode method so far is
           'json'."""
        # encode result if requested
        if encode == 'json':
            result = json.dumps(result)
        elif not encode is None:
            raise Exception('No such encoder: %s' % encode)
        
        # send "result" IPC message to manager
        AMQPProcessManagerIPC._send_ipc_message(['result', result])
    
    @staticmethod
    def send_heartbeat():
        """Sends a heartbeat to the manager to signal that this process needs more
           time to complete and is not dead yet. Avoid calling this in loops or
           other code segments of high complexity so watch dog still triggers when
           this process actually hangs."""
        AMQPProcessManagerIPC._send_ipc_message(['heartbeat'])

# some basic examples on how to use the class:
if __name__ == '__main__':
    ipc = AMQPProcessManagerIPC
    ipc.init()
    ipc.send_heartbeat()
    ipc.send_result('test')
    ipc.send_result({'a': [1,2,3], 'b': {'b1': 1.0, 'b2': 1.1, 'b3': 5, 'b4': 'b4value'}, 'c': None, 'd': True}, 'json')
    print('received: %s' % ipc.receive_message(decode='json'))
