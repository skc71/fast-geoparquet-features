from typing import Self

from pydantic import BaseModel


class BBox(BaseModel):
    xmin: float
    ymin: float
    xmax: float
    ymax: float

    @classmethod
    def from_str(cls, bbox: str) -> Self:
        if len((coords := bbox.split(","))) != 4:
            raise ValueError("bbox must be 4 comma-separated floats")
        else:
            try:
                xmin, ymin, xmax, ymax = tuple((float(c.strip()) for c in coords))
            except ValueError:
                raise ValueError("all bbox values must be floats")

        return cls(xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax)

    def to_sql(self) -> str:
        return " AND ".join(
            [
                f"bbox.xmax >= {self.xmin}",
                f"bbox.xmin <= {self.xmax}",
                f"bbox.ymax >= {self.ymin}",
                f"bbox.ymin <= {self.ymax}",
            ]
        )
