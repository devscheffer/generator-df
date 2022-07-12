# %%
from functools import reduce
import pandas as pd

# %%


def _list_to_df(
    *,
    column: str,
    dataset: dict,
) -> pd.DataFrame:
    data = dataset.get("data")
    type = dataset.get("type")
    df = pd.DataFrame(
        data=data,
        dtype=type,
        columns=[column],
    )
    return df


def _cross_join_df(
    *,
    df1: pd.DataFrame,
    df2: pd.DataFrame,
) -> pd.DataFrame:
    df = pd.merge(
        df1,
        df2,
        how="cross",
    )
    return df


def df_generate(
    *,
    base_dict: dict,
) -> pd.DataFrame:
    df_create = map(
        lambda x: _list_to_df(
            column=x,
            dataset=base_dict[x],
        ),
        base_dict,
    )
    df_reduce = reduce(
        lambda x, y: _cross_join_df(
            df1=x,
            df2=y,
        ),
        df_create,
    )
    return df_reduce


#%%


if __name__ == "__main__":
    base_dict = {
        "c1": {
            "data": [
                0,
                1,
                None,
            ],
            "type": int,
        },
        "c2": {
            "data": [
                0,
                1,
                2,
                None,
            ],
            "type": int,
        },
    }
    df = df_generate(
        base_dict=base_dict,
    )
    # %%

    df.shape
    df.info()
    df.describe()
    df.groupby("c1")["c2"].count().head()


# %%
