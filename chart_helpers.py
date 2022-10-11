import pandas as pd, numpy as np, altair as alt

def line_chart(df, x, y, tooltip_sup=[], st=None):
    chart = alt.Chart(df).mark_line(point=alt.OverlayMarkDef(size=80.0, filled=False, fill='white')).encode(
        x=x,
        y=y, 
        #color=color,
        tooltip=[x,y] + tooltip_sup
    ).properties( width=1300,  height=450)
    if st:
        st.altair_chart(chart)
    else :
        return chart
    

def multi_line_chart(df, x, y, color=None, tooltip_sup=[], balls='medium', st=None):
    if balls==True:
        balls=alt.OverlayMarkDef(size=80.0, filled=False, fill='white')
    elif balls=='small':
        balls=alt.OverlayMarkDef(size=20.0, filled=False, fill='white')
    elif balls=='medium':
        balls=alt.OverlayMarkDef(size=30.0, filled=False, fill='white')
    else:
        balls=False
    chart = alt.Chart(df).mark_line(point=balls).encode(
        x=x,
        y=y, 
        color=color,
        tooltip=[x,y] + tooltip_sup
    ).properties( width=1300,  height=650)
    if st:
        st.altair_chart(chart)
    else :
        return chart
    
def bar_chart(df, x, y, color, tooltip_sup=[], scheme='lighttealblue', st=None, limit_colors= 10, limit_agg='sum'): #(couleurs sympa : purpleblue  lighttealblue   blueorange   redblue)
    df = df.copy()
    #limit nb colors
    if df[color].nunique() > limit_colors:
        color_list = pd.DataFrame(df.groupby(color).agg({y:limit_agg})).sort_values(by=y, ascending=False).index[:limit_colors]
        df[color][~df[color].isin(color_list)] = 'autres'
        df = df.groupby([x,color]).agg({y:limit_agg}).reset_index()
        df[color] = df[color].astype(str)
    #return df
    chart = alt.Chart(df).mark_bar().encode(x=x, y=y, color=\
                        alt.Color(color,  scale=alt.Scale(scheme=scheme)), \
             tooltip=[x,y,color] + tooltip_sup).properties( width=1300,  height=450)
    
    if st:
        st.altair_chart(chart)
    else :
        return chart
    
def get_parquet_asset(asset_name):
    return pd.read_parquet('/workspace/gitignore_data/'+asset_name.replace(' ','')+'.parquet')
