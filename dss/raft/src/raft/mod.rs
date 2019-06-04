use std::sync::{Arc, Mutex, mpsc::{self, Sender}};
use std::thread;
use std::time::{Duration, SystemTime};

use futures::Future;
use futures::sync::mpsc::UnboundedSender;
use labcodec;
use labrpc::RpcFuture;

use rand::{thread_rng, Rng};

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
pub mod service;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use self::service::*;

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Role {
    Leader, Follower, Candidate,
}

#[derive(Debug)]
pub struct Log {
    pub term: u64,
    pub index: u64,
}

const MIN_ELECTION_TIMEOUT: u32 = 150;
const MAX_ELECTION_TIMEOUT: u32 = 300;
const RPC_TIMEOUT: Duration = Duration::from_millis(50);
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    role: Role,
    election_timeout: Duration,
    last_heartbeat: SystemTime,

    current_term: u64,
    voted_for: Option<u64>,
    log: Vec<Log>,

    commit_index: u64,
    last_applied: u64,

    next_index: Vec<u64>,
    match_index: Vec<u64>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();
        let mut rng = thread_rng();
        let election_timeout = Duration::from_millis(rng.gen_range(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT) as u64);

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            role: Role::Follower,
            election_timeout,
            last_heartbeat: SystemTime::now(),
            current_term: 0,
            voted_for: None,
            log: vec![],
            commit_index: 0,
            last_applied: 0,
            next_index: vec![],
            match_index: vec![],
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    fn downgrade_to_follower(&mut self) {
        self.role = Role::Follower;
        self.voted_for = None;
        self.last_heartbeat = SystemTime::now();
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/mod.rs for more details.
    fn send_request_vote(&self, server: usize, args: &RequestVoteArgs, sender: Sender<Result<RequestVoteReply>>) {
        let peer = &self.peers[server];
        peer.spawn(
            peer.request_vote(args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    sender.send(res);
                    Ok(())
                }),
        );
    }

    fn send_append_entries(&self, server: usize, args: &AppendEntriesArgs, sender: Sender<Result<AppendEntriesReply>>) {
        let peer = &self.peers[server];
        peer.spawn(
            peer.append_entries(args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    sender.send(res);
                    Ok(())
                }),
        );
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Mutex<Raft>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let node = Node {
            raft: Arc::new(Mutex::new(raft)),
        };
        node.spawn_candidate();
        node
    }

    fn spawn_candidate(&self) -> thread::JoinHandle<()> {
        let raft = Arc::clone(&self.raft);
        thread::spawn(move || {
            let (election_timeout, n_peers) = {
                let raft = raft.lock().unwrap();
                (raft.election_timeout, raft.peers.len())
            };
            loop {
                let (elapsed, role) = {
                    let raft = raft.lock().unwrap();
                    (raft.last_heartbeat.elapsed().unwrap(), raft.role)
                };
                if role == Role::Leader {
                    //raft.last_heartbeat = SystemTime::now();
                    let (receiver, current_term) = {
                        let (tx, rx) = mpsc::channel();
                        let mut raft = raft.lock().unwrap();
                        raft.last_heartbeat = SystemTime::now();
                        let request = AppendEntriesArgs{
                            term: raft.current_term,
                            leader_id: raft.me as u64,
                            prev_log_index: 1,
                            prev_log_term: 1,
                            entries: vec![],
                            leader_commit: raft.commit_index,
                        };
                        for i in 0..n_peers {
                            if i == raft.me {
                                continue;
                            }
                            raft.send_append_entries(i, &request, tx.clone());
                        }
                        (rx, raft.current_term)
                    };
                    let mut latest_term = 0;
                    for _ in 0..n_peers - 1 {
                        if let Ok(Ok(resp)) = receiver.recv_timeout(RPC_TIMEOUT) {
                            if resp.term > latest_term {
                                latest_term = resp.term;
                            }
                        }
                    }
                    if latest_term > current_term {
                        let mut raft = raft.lock().unwrap();
                        raft.downgrade_to_follower();
                        continue
                    } else {
                        thread::sleep(HEARTBEAT_INTERVAL);
                        continue;
                    }
                }
                if let Some(sleep_time) = election_timeout.checked_sub(elapsed) {
                    thread::sleep(sleep_time);
                    continue;
                }
                let receiver = {
                    let (tx, rx) = mpsc::channel();
                    let mut raft = raft.lock().unwrap();
                    raft.current_term += 1;
                    raft.voted_for = Some(raft.me as u64);
                    raft.last_heartbeat = SystemTime::now();
                    let request = RequestVoteArgs{
                        term: raft.current_term,
                        candidate_id: raft.me as u64,
                        last_log_index: 1,
                        last_log_term: 1,
                    };
                    for i in 0..n_peers {
                        if i == raft.me {
                            continue;
                        }
                        raft.send_request_vote(i, &request, tx.clone());
                    }
                    rx
                };
                let mut votes = 1;
                let mut latest_term = 0;
                for _ in 0..n_peers - 1 {
                    if let Ok(Ok(resp)) = receiver.recv_timeout(RPC_TIMEOUT) {
                        if resp.term > latest_term {
                            latest_term = resp.term;
                        }
                        if resp.vote_granted {
                            votes += 1;
                        }
                    }
                }
                let mut raft = raft.lock().unwrap();
                if raft.current_term < latest_term {
                    raft.current_term = latest_term;
                    continue;
                } else if votes * 2 <= n_peers {
                    continue;
                }
                raft.role = Role::Leader;
            }
        })
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        unimplemented!()
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        self.raft.lock().unwrap().current_term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        self.raft.lock().unwrap().role == Role::Leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        let mut raft = self.raft.lock().unwrap();
        if args.term < raft.current_term {
            return Box::new(futures::future::result(Ok(
                RequestVoteReply{term: raft.current_term, vote_granted: false}
            )));
        }
        if args.term > raft.current_term {
            raft.current_term = args.term;
            raft.downgrade_to_follower();
        }
        let vote_granted = match raft.voted_for {
            Some(candidate) => candidate == args.candidate_id,
            None => args.last_log_index >= raft.commit_index,
        };
        if vote_granted {
            raft.last_heartbeat = SystemTime::now();
        }
        Box::new(futures::future::result(Ok(
            RequestVoteReply{term: raft.current_term, vote_granted}
        )))
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        let mut raft = self.raft.lock().unwrap();
        if args.term < raft.current_term {
            return Box::new(futures::future::result(Ok(
                AppendEntriesReply{term: raft.current_term, success: false}
            )));
        }
        if args.term > raft.current_term {
            raft.current_term = args.term;
            raft.downgrade_to_follower();
        }
        raft.last_heartbeat = SystemTime::now();
        Box::new(futures::future::result(Ok(
            AppendEntriesReply{term: raft.current_term, success: false}
        )))
    }
}
