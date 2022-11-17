import dataclasses
import datetime
import json
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


class DigestType(Enum):
    LongOneCell = "1-cell-repetition"
    ShortOneCell = "1-cell-repetition"
    ShortTwoCell = "2-cell-flapping"
    ShortThreeCell = "3-cell-flapping"


@dataclass
class Digest:
    start_time: datetime.datetime
    cells: set[str]
    num_events: int
    num_cells: int
    type: DigestType = DigestType.ShortOneCell
    end_time: Optional[datetime.datetime] = None


class DigestEncoder(json.JSONEncoder):
    """JSON Digest encoder."""

    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        elif isinstance(obj, set):
            return tuple(obj)
        elif isinstance(obj, DigestType):
            return obj.value
        elif isinstance(obj, Digest):
            dataclasses.asdict(obj)


def create_digest(time, cell):
    return Digest(start_time=time, cells=set([cell]), num_events=1, num_cells=1)


class Digestor:
    def __init__(
        self,
        short_dt=15,
        long_dt=8 * 60 * 60,
        cutoff=24 * 60 * 60,
        t0=datetime.datetime(1500, 1, 1, 0, 0, 0),
    ):
        """State-machine that reads sequences of events and produces a sequence of digests."""
        self.current_digest = None
        self.last_time = t0
        self.last_cell = "unknown_cell"
        self.short_dt = short_dt
        self.long_dt = long_dt
        self.cutoff = cutoff
        return

    def close_and_start(self, time, cell) -> Digest:
        """Close current digest, return it, and create a new one."""
        previous_digest = self.close_digest()
        if previous_digest and previous_digest.num_events > 1:
            self.current_digest = create_digest(self.last_time, self.last_cell)
            self.process_event(time, cell)
        else:
            self.current_digest = create_digest(time, cell)
            self.last_time = time
            self.last_cell = cell
        return previous_digest

    def close_digest(self) -> Digest:
        """Close and return current digest."""
        previous_digest = self.current_digest
        if previous_digest:
            previous_digest.end_time = self.last_time
        self.current_digest = None
        return previous_digest

    def continue_digest(self, time) -> None:
        """Add event to the current digest."""
        self.last_time = time
        self.current_digest.num_events += 1

    def add_cell(self, cell) -> None:
        """Add cell to the current digest."""
        self.current_digest.cells.add(cell)
        self.current_digest.num_cells += 1

    def process_event(self, time, cell, last_time=None) -> Optional[Digest]:
        """Advance the state by processing the current event.

        This function encodes the logic of digest generation by
        defining the transitions between types of digests. If the
        state cannot move to a valid state, then the current digest
        is closed (ending at the previous event) and returned.
        If no digest is closed then nothing is returned.

        The transition between states depends on the amount of time
        between the last and current event (`dt`) and wether the
        current event's cell is in the set of cells in the digest
        (`dc=0`) or not (`dc=1`).

        Graph representation of state transitions:

        ```mermaid
        graph TD
            EmptyDigest -- true --> 1CF
            1CF -- "(T2 < dt < T1) & dc=0" --> LongDigest -- "dt < T1 & dc=0" --> LongDigest -- else --> FinishDigest
            1CF -- "dt < T2 & dc=0" --> 1CF -- else --> FinishDigest
            2CF --"dt < T2 & dc = 0" --> 2CF -- else --> FinishDigest
            1CF --"dt < T2 & dc = 1" --> 2CF
            2CF --"dt < T2 & dc = 1"--> 3CF --"dt < T2 & dc = 0"--> 3CF --else--> FinishDigest
        ```
        """
        if self.current_digest:
            dt = (time - (last_time or self.last_time)).total_seconds()
            if dt < 0:
                raise Exception(
                    f"events are not ordered in time. Last event was at {self.last_time} and the current one at {time}."
                )
            if self.current_digest.type == DigestType.ShortOneCell:
                if dt < self.short_dt and cell in self.current_digest.cells:
                    self.continue_digest(time)
                elif dt < self.short_dt:
                    self.continue_digest(time)
                    self.add_cell(cell)
                    self.current_digest.type = DigestType.ShortTwoCell
                elif dt < self.long_dt and cell in self.current_digest.cells:
                    self.continue_digest(time)
                    self.current_digest.type = DigestType.LongOneCell
                else:
                    return self.close_and_start(time, cell)
            elif self.current_digest.type == DigestType.ShortTwoCell:
                if dt < self.short_dt and cell in self.current_digest.cells:
                    self.continue_digest(time)
                elif dt < self.short_dt:
                    self.continue_digest(time)
                    self.add_cell(cell)
                    self.current_digest.type = DigestType.ShortThreeCell
                else:
                    return self.close_and_start(time, cell)
            elif self.current_digest.type == DigestType.ShortThreeCell:
                if dt < self.short_dt and cell in self.current_digest.cells:
                    self.continue_digest(time)
                else:
                    return self.close_and_start(time, cell)
            elif self.current_digest.type == DigestType.LongOneCell:
                if dt < self.long_dt and cell in self.current_digest.cells:
                    self.continue_digest(time)
                else:
                    return self.close_and_start(time, cell)
            else:
                raise NotImplementedError(
                    f"unexpected DigestType: {self.current_digest.type}"
                )

            if (
                self.current_digest.start_time
                and (time - self.current_digest.start_time).total_seconds()
                > self.cutoff
            ):
                return self.close_and_start(time, cell)
        else:
            self.close_and_start(time, cell)
        return None

    def process_events(self, ordered_events) -> List[Digest]:
        return self._process_events_dict(ordered_events)

    def _process_events_dict(self, ordered_events) -> List[Digest]:
        """Process a sequence of ordered events.

        Returns the list of _closed_ digests.
        """
        digests = [
            self.process_event(event["time"], event["cell"]) for event in ordered_events
        ]
        return [digest for digest in digests if digest]

    def _process_events_iter(self, ordered_times, ordered_cells) -> List[Digest]:
        """Process a sequence of ordered events.

        Returns the list of _closed_ digests.
        """
        if len(ordered_times) != len(ordered_cells):
            raise Exception(
                f"Unequal number of entries: {len(ordered_times)} times but {len(ordered_cells)} cells"
            )
        digests = [
            self.process_event(
                ordered_times[i],
                ordered_cells[i],
                last_time=(
                    ordered_times[i - 1]
                    if i > 0
                    else datetime.datetime(1900, 1, 1, 0, 0, 0)
                ),
            )
            for i in range(len(ordered_times))
        ]
        return [digest for digest in digests if digest]


def digest_generation_dict(ordered_events):
    digestor = Digestor()
    digests = digestor._process_events_dict(ordered_events)
    digests.append(digestor.close_digest())
    return digests


def digest_generation_iter(ordered_times, ordered_cells):
    digestor = Digestor()
    digests = digestor._process_events_iter(ordered_times, ordered_cells)
    digests.append(digestor.close_digest())
    return digests


digest_generation = digest_generation_dict
