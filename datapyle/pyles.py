
import time
import datetime
from datapyle.sqlaimports import *
from sqlalchemy.orm import sessionmaker
import threading
from Queue import PriorityQueue

# =========================================================================== #

class Pyle(object):
    id = Column(Integer, primary_key=True)
    fetch_time = Column(DateTime,default=datetime.datetime.utcnow)
    sampling_interval = datetime.timedelta(hours=1)
    _fetch_duration = None
    
    @declared_attr
    def __tablename__(cls):
        return cls.__name__


class Pyler(object):
    PyleBase = declarative_base()
    
    def __init__(self, pyle_path):
        self.db_url = "sqlite:///{}".format(pyle_path)
        self.pq = PriorityQueue()
        
        self.engine = sa.create_engine(self.db_url)
        self.Session = sessionmaker(expire_on_commit=False, bind=self.engine)
    
    def queue_pyle(self, pyle_class, session):
        last_sampled = session.query(pyle_class.fetch_time)\
                .order_by(sa.desc(pyle_class.fetch_time)).first()
        right_now = datetime.datetime.today()
        if last_sampled is None:
            self.pq.put((right_now, pyle_class))
        else:
            self.pq.put((right_now+pyle_class.sampling_interval)) 
    
    def higher_and_deeper(self):
        self.PyleBase.metadata.create_all(self.engine)
        session = self.Session()
        start_time = time.time()
        pyles = []
        for pyle in self.PyleBase._decl_class_registry.values():
            try:
                if issubclass(pyle, self.PyleBase):
                    pyles.append(pyle)
            except TypeError:
                pass
        #import pdb; pdb.set_trace()
        for pc in pyles:
            self.queue_pyle(pc, session)
        while True:
            priority, pyle = self.pq.get()
            time_till_sample = priority-datetime.datetime.today()
            sleep_sec = time_till_sample.total_seconds()
            if not pyle._fetch_duration is None:
                sleep_sec -= pyle._fetch_duration
            if sleep_sec > 0:
                time.sleep(sleep_sec)
            fetch_stime = time.time()
            new_datum = pyle.fetch(session)
            fetch_etime = time.time()
            fetch_duration = fetch_etime-fetch_stime
            if pyle._fetch_duration is None:
                pyle._fetch_duration = fetch_duration
            else:
                alpha = 0.2
                duration_avg = pyle._fetch_duration*(1-alpha) + alpha*fetch_duration
                pyle._fetch_duration = duration_avg
            self.pq.put((priority+pyle.sampling_interval, pyle))
            session.add(new_datum)
            session.commit()
        
