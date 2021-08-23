# DS_toys

some feedback
1. MapReduce:
here can use many ways for master to coordinate with worker, eg: channel.
the way I use is just like a DFA, and due to the RPC type the worker send and
the state the master is. we shall give worker different works or just let them idle.

2. Raft
  a. leader election
  on this stage, I fail many times, experience are below:
     I. at first, I let servers and leader send RPC not parallel, which brings many problems
        aftert that, I change into go's WaitGroup, but ignore that network may fail, that election can't
        be finished in a given time. finally, I change to that depart one part into goroutine's sync RPC.
     II. at first, the timeticker I design is problematic. I use a restart flag to distinguish if a server has receive
         RPC and can be reset election timeout. this makes code logic very complex. finally, use a timestamp.
     III. some details in the thesis are mistaken by me, eg : the voteGrant condition
