use std::cmp::min;
use std::sync::{
    mpsc::{self, Sender},
    Arc, Mutex,
};
use std::thread;
use std::time::{Duration, SystemTime};

use futures::sync::mpsc::UnboundedSender;
use futures::Future;
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
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug)]
pub struct Log {
    pub term: u64,

    pub data: Vec<u8>,
}

const MIN_ELECTION_TIMEOUT: u64 = 150;
const MAX_ELECTION_TIMEOUT: u64 = 300;
const RPC_TIMEOUT: Duration = Duration::from_millis(50);
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);
const APPLY_INTERVAL: Duration = Duration::from_millis(100);

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
    killed: bool,

    current_term: u64,
    voted_for: Option<u64>,
    log: Vec<Log>,

    commit_index: u64,
    last_applied: u64,

    next_index: Vec<u64>,
    match_index: Vec<u64>,

    apply_ch: UnboundedSender<ApplyMsg>,
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
        let election_timeout =
            Duration::from_millis(rng.gen_range(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT));

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            role: Role::Follower,
            election_timeout,
            last_heartbeat: SystemTime::now(),
            killed: false,
            current_term: 0,
            voted_for: None,
            log: vec![],
            commit_index: 0,
            last_applied: 0,
            next_index: vec![],
            match_index: vec![],
            apply_ch,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        self.role == Role::Leader
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
    fn send_request_vote(
        &self,
        server: usize,
        args: &RequestVoteArgs,
        sender: Sender<Result<RequestVoteReply>>,
    ) {
        debug!("node {} -> {} {:?}", self.me, server, args);
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

    fn send_append_entries(
        &self,
        server: usize,
        args: &AppendEntriesArgs,
        sender: Sender<Result<AppendEntriesReply>>,
    ) {
        debug!("node {} -> {} {:?}", self.me, server, args);
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

    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if !self.is_leader() {
            return Err(Error::NotLeader);
        }
        debug!("node {} term {} starts {:?}", self.me, self.current_term, command);
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).
        let term = self.current_term;
        let index = self.log.len() + 1;
        self.log.push(Log{term, data: buf});
        Ok((index as u64, term))
    }
}

fn candidate_procedure(raft: &Arc<Mutex<Raft>>, sync_threads: &Arc<Vec<thread::JoinHandle<()>>>) {
    let (election_timeout, elapsed, n_peers) = {
        let mut raft = raft.lock().unwrap();
        if raft.role == Role::Leader {
            raft.last_heartbeat = SystemTime::now();
        }
        (
            raft.election_timeout,
            raft.last_heartbeat.elapsed().unwrap(),
            raft.peers.len(),
        )
    };
    if let Some(sleep_time) = election_timeout.checked_sub(elapsed) {
        thread::sleep(sleep_time);
        return;
    }
    let election_time = SystemTime::now();
    let receiver = {
        let mut raft = raft.lock().unwrap();
        raft.role = Role::Candidate;
        raft.current_term += 1;
        info!("node {} term {} inits election", raft.me, raft.current_term);
        raft.voted_for = Some(raft.me as u64);
        raft.last_heartbeat = election_time;
        let last_log_term = if !raft.log.is_empty() {
            raft.log[raft.log.len() - 1].term
        } else {
            0
        };
        let (tx, rx) = mpsc::channel();
        let request = RequestVoteArgs {
            term: raft.current_term,
            candidate_id: raft.me as u64,
            last_log_index: raft.log.len() as u64,
            last_log_term,
        };
        for i in 0..n_peers {
            if i == raft.me {
                continue;
            }
            raft.send_request_vote(i, &request, tx.clone());
        }
        rx
    };
    let mut votes = 0;
    let mut latest_term = 0;
    for _ in 0..n_peers - 1 {
        if let Some(wait_time) = election_timeout.checked_sub(election_time.elapsed().unwrap()) {
            if let Ok(Ok(resp)) = receiver.recv_timeout(wait_time) {
                if resp.term > latest_term {
                    latest_term = resp.term;
                }
                if resp.vote_granted {
                    votes += 1;
                }
            }
        } else {
            break;
        }
    }
    let mut raft = raft.lock().unwrap();
    if let Some(c) = raft.voted_for {
        if c == raft.me as u64 {
            votes += 1;
        }
    }
    info!("node {} term {} received {}/{} votes", raft.me, raft.current_term, votes, n_peers);
    if raft.current_term < latest_term {
        raft.downgrade_to_follower();
    } else if votes * 2 >= n_peers {
        raft.role = Role::Leader;
        raft.next_index = vec![raft.log.len() as u64 + 1; n_peers];
        raft.match_index = vec![0; n_peers];
        for t in sync_threads.iter() {
            t.thread().unpark();
        }
    }
}

fn sync_procedure(raft: &Arc<Mutex<Raft>>, index: usize) {
    let role = {
        let raft = raft.lock().unwrap();
        raft.role
    };
    if role != Role::Leader {
        thread::park();
        return;
    }
    let last_heartbeat = SystemTime::now();
    let (receiver, last_index) = {
        let raft = raft.lock().unwrap();
        debug!("node {} log {:?}", raft.me, raft.log);
        let next_index = raft.next_index[index] as usize;
        let (prev_log_index, prev_log_term) = if next_index > 1 {
            (next_index as u64 - 1, raft.log[next_index - 2].term)
        } else {
            (0, 0)
        };
        let mut entries = vec![];
        for index in next_index..=raft.log.len() {
            let log = &raft.log[index - 1];
            entries.push(LogPb{term: log.term, data: log.data.clone()});
        }
        let (tx, rx) = mpsc::channel();
        raft.send_append_entries(index,
            &AppendEntriesArgs{
                term: raft.current_term,
                leader_id: raft.me as u64,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: raft.commit_index,
            }, tx);
        (rx, raft.log.len() as u64)
    };
    let reply = receiver.recv().unwrap();
    if reply.is_err() {
        return;
    }
    let reply = reply.unwrap();
    let mut raft = raft.lock().unwrap();
    if reply.term > raft.current_term {
        raft.downgrade_to_follower();
        return;
    } else if reply.term < raft.current_term {
        return;
    }
    if !reply.success {
        if raft.next_index[index] > 1 {
            raft.next_index[index] -= 1;
        }
        return;
    }
    raft.match_index[index] = last_index;
    raft.next_index[index] = last_index + 1;
    let mut match_indices = Vec::with_capacity(raft.peers.len());
    for (i, &index) in raft.match_index.iter().enumerate() {
        if i == raft.me {
            continue;
        }
        match_indices.push(index);
    }
    match_indices.sort_unstable();
    for index in match_indices[0..=match_indices.len() / 2].iter().rev() {
        if *index == 0 {
            break;
        }
        if raft.log[*index as usize - 1].term == raft.current_term {
            raft.commit_index = *index;
            break;
        }
    }
    if raft.log.len() as u64 >= raft.next_index[index] {
        return;
    }
    if let Some(sleep_time) = HEARTBEAT_INTERVAL.checked_sub(last_heartbeat.elapsed().unwrap()) {
        thread::park_timeout(sleep_time);
    }
}

fn apply_procedure(raft: &Arc<Mutex<Raft>>) {
    thread::sleep(APPLY_INTERVAL);
    loop {
        let mut raft = raft.lock().unwrap();
        if raft.commit_index > raft.last_applied {
            raft.last_applied += 1;
            if raft.apply_ch.unbounded_send(ApplyMsg{
                command_valid: true,
                command: raft.log[raft.last_applied as usize - 1].data.clone(),
                command_index: raft.last_applied,
            }).is_err() {
                raft.last_applied -= 1;
            }
        } else {
            break;
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
    election_heartbeat_thread: Arc<thread::JoinHandle<()>>,
    sync_threads: Arc<Vec<thread::JoinHandle<()>>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        let n_peers = raft.peers.len();
        let me = raft.me;
        let raft = Arc::new(Mutex::new(raft));
        let sync_threads = {
            let mut threads = vec![];
            for i in 0..n_peers {
                if i == me {
                    continue;
                }
                let raft = Arc::clone(&raft);
                threads.push(thread::spawn(move || {
                    loop {
                        sync_procedure(&raft, i);
                        if raft.lock().unwrap().killed {
                            break;
                        }
                    }
                }));
            }
            Arc::new(threads)
        };
        let election_heartbeat_thread = {
            let raft = Arc::clone(&raft);
            let sync_threads = Arc::clone(&sync_threads);
            Arc::new(thread::spawn(move || {
                loop {
                    {
                        if raft.lock().unwrap().killed {
                            break;
                        }
                    }
                    candidate_procedure(&raft, &sync_threads);
                }
            }))
        };
        {
            let raft = Arc::clone(&raft);
            thread::spawn(move || {
                loop {
                    {
                        if raft.lock().unwrap().killed {
                            break;
                        }
                    }
                    apply_procedure(&raft);
                }
            });
        }
        Node {
            raft,
            election_heartbeat_thread,
            sync_threads,
        }
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
        let result = self.raft.lock().unwrap().start(command)?;
        self.election_heartbeat_thread.thread().unpark();
        Ok(result)
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
        self.raft.lock().unwrap().killed = true;
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        let mut raft = self.raft.lock().unwrap();
        if args.term < raft.current_term || raft.last_heartbeat.elapsed().unwrap().checked_sub(Duration::from_millis(MIN_ELECTION_TIMEOUT)).is_none() {
            return Box::new(futures::future::result(Ok(RequestVoteReply {
                term: raft.current_term,
                vote_granted: false,
            })));
        }
        if args.term > raft.current_term {
            raft.current_term = args.term;
            raft.downgrade_to_follower();
        }
        let vote_granted = match raft.voted_for {
            Some(candidate) => candidate == args.candidate_id,
            None => if raft.log.is_empty() || args.last_log_index == 0 {
                args.last_log_index >= raft.log.len() as u64
            } else {
                args.last_log_term >= raft.log[min(raft.log.len(), args.last_log_index as usize) - 1].term
            }
        };
        if vote_granted {
            debug!("node {} grant vote", raft.me);
            raft.last_heartbeat = SystemTime::now();
        }
        Box::new(futures::future::result(Ok(RequestVoteReply {
            term: raft.current_term,
            vote_granted,
        })))
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        let mut raft = self.raft.lock().unwrap();
        if args.term < raft.current_term {
            return Box::new(futures::future::result(Ok(AppendEntriesReply {
                term: raft.current_term,
                success: false,
            })));
        }
        if args.term > raft.current_term {
            raft.current_term = args.term;
            raft.downgrade_to_follower();
        }
        raft.last_heartbeat = SystemTime::now();
        debug!("node {} hb", raft.me);

        if args.prev_log_index > 0 && (raft.log.len() < args.prev_log_index as usize || raft.log[args.prev_log_index as usize - 1].term != args.prev_log_term) {
            return Box::new(futures::future::result(Ok(AppendEntriesReply {
                term: raft.current_term,
                success: false,
            })));
        }
        let n_entries = args.entries.len();
        for (i, log) in args.entries.into_iter().enumerate() {
            let new_index = args.prev_log_index as usize + 1 + i;
            if new_index > raft.log.len() {
                raft.log.push(Log{term: log.term, data: log.data});
            } else if raft.log[new_index - 1].term != log.term {
                raft.log[new_index - 1] = Log{term: log.term, data: log.data};
            }
        }
        if raft.log.len() > args.prev_log_index as usize + n_entries {
            raft.log.drain(args.prev_log_index as usize + n_entries..);
        }
        debug!("node {} log {:?}", raft.me, raft.log);
        if args.leader_commit > raft.commit_index {
            raft.commit_index = min(args.leader_commit, raft.log.len() as u64);
        }
        Box::new(futures::future::result(Ok(AppendEntriesReply {
            term: raft.current_term,
            success: true,
        })))
    }
}
