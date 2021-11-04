# Key Points & Implementations
[Official Site](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)

+ Coordinator and Worker programs are required. Coordinator program is responsible for the Scheduling of the whole job while the workers focus on the processing of data (map & reduce).
+ Programs communicate via RPC.
+ Fault tolerance is required, the coordinator should handle the task to another worker if the original worker doesn't respond in time.