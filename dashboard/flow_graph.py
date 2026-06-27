"""
Sentinel — Animated data-architecture canvas (dashboard component).

Renders the *real* pipeline data architecture as a self-contained HTML5-canvas
animation (no external libraries / CDN):

    PaySim source ─▶ raw ─▶ cleaned ─▶ enriched ─▶ fraud_features ─▶ DuckDB

with the two observability streams tapped off every stage and fanning into the
Detection engine:

* **data-metrics** stream  (cyan; turns **amber** when a data check fires)
* **operational** stream    (violet; turns **red** when a job fails / is skipped)

Dense particles stream continuously through the pipes; a faulty stage glows and
its colour propagates downstream, and a dashed-red **correlation** arc is drawn
from an operational-error stage to the data error it caused.

Public API: :func:`render_flow` — takes the same ``stage_states`` the dashboard
already builds and returns an HTML string for ``st.components.v1.html``.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List


def render_flow(stage_states: List[Dict[str, Any]], height: int = 460) -> str:
    """Return HTML rendering the animated data-architecture canvas.

    Parameters
    ----------
    stage_states:
        Ordered (pipeline order) list of
        ``{"id", "label", "status": healthy|data_error|pipeline_error,
           "badges": [str], "correlated_from": str|None}``.
    height:
        Pixel height of the component.
    """
    html = _TEMPLATE.replace("__PAYLOAD__", json.dumps(stage_states))
    html = html.replace("__HEIGHT__", str(int(height)))
    return html


_TEMPLATE = r"""
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"/>
<style>
  html,body{margin:0;padding:0;background:#0e1117;overflow:hidden;}
  #wrap{position:relative;width:100%;height:__HEIGHT__px;}
  canvas{display:block;}
  #legend{position:absolute;left:10px;bottom:8px;font:12px ui-sans-serif,system-ui;color:#94a3b8;}
  #legend b{color:#cbd5e1;font-weight:600;}
  .sw{display:inline-block;width:10px;height:10px;border-radius:50%;vertical-align:middle;margin:0 4px 0 12px;}
</style></head>
<body>
<div id="wrap">
  <canvas id="c"></canvas>
  <div id="legend">
    <span class="sw" style="background:#38bdf8"></span>data metrics
    <span class="sw" style="background:#a78bfa"></span>operational
    <span class="sw" style="background:#f59e0b"></span>data error
    <span class="sw" style="background:#ef4444"></span>pipeline error
    &nbsp;&nbsp;<b style="color:#ef4444">╌▶</b> caused-by
  </div>
</div>
<script>
(function(){
  const STAGES = __PAYLOAD__;
  const DPR = window.devicePixelRatio || 1;
  const wrap = document.getElementById('wrap');
  const canvas = document.getElementById('c');
  const ctx = canvas.getContext('2d');

  const C = {flow:'#38bdf8', ops:'#a78bfa', amber:'#f59e0b', red:'#ef4444',
            green:'#22c55e', dim:'#334155', text:'#e2e8f0', sub:'#94a3b8'};
  const RANK = {healthy:0, data_error:1, pipeline_error:2};
  const sev  = s => RANK[s]||0;
  const stageColor = s => s==='pipeline_error'?C.red : s==='data_error'?C.amber : C.green;

  let W=0,H=0, nodes=[], stageIdx=[], detect=null;

  function layout(){
    const L=72, R=64, midY=H*0.30;
    const names = ['SOURCE'].concat(STAGES.map(s=>s.label)).concat(['WAREHOUSE']);
    const n = names.length;                         // 6
    nodes = names.map((nm,i)=>({
      x: L + i*((W-L-R)/(n-1)), y: midY,
      w: (i===0||i===n-1)?92:118, h:46, label:nm,
      kind: i===0?'source':(i===n-1?'warehouse':'stage'),
      st: (i>0 && i<n-1) ? STAGES[i-1] : null
    }));
    stageIdx = nodes.map((nd,i)=>nd.kind==='stage'?i:-1).filter(i=>i>=0);
    detect = {x:W*0.5, y:H*0.84, w:230, h:48, label:'DETECTION'};
  }

  function resize(){
    const r = wrap.getBoundingClientRect(); W=r.width; H=r.height;
    canvas.width=W*DPR; canvas.height=H*DPR; canvas.style.width=W+'px'; canvas.style.height=H+'px';
    ctx.setTransform(DPR,0,0,DPR,0,0); layout();
  }
  window.addEventListener('resize', resize);

  // worst severity across stages -> taints downstream pipe + detection
  function worstUpTo(stageNo){ let w=0; for(let i=0;i<=stageNo && i<STAGES.length;i++) w=Math.max(w,sev(STAGES[i].status)); return w; }
  const colorForRank = r => r===2?C.red : r===1?C.amber : C.flow;

  // ── particles ─────────────────────────────────────────────────────────
  const main = [];   // along source->...->warehouse
  for(let i=0;i<46;i++) main.push({p:Math.random(), v:0.0016+Math.random()*0.0014});
  // taps: each stage -> detection, two streams (data + ops)
  const taps = [];
  STAGES.forEach((s,si)=>{ for(let k=0;k<3;k++){
    taps.push({si, stream:'data', t:Math.random(), v:0.006+Math.random()*0.004});
    taps.push({si, stream:'ops',  t:Math.random(), v:0.006+Math.random()*0.004});
  }});

  function pathPoint(p){ // p in [0,1] across the whole main pipe
    const segs = nodes.length-1, fp = p*segs; let i=Math.floor(fp); if(i>=segs)i=segs-1;
    const a=nodes[i], b=nodes[i+1], t=fp-i;
    return {x:a.x+(b.x-a.x)*t, y:a.y+(b.y-a.y)*t, seg:i};
  }

  function rrect(x,y,w,h,r){ ctx.beginPath(); ctx.moveTo(x+r,y); ctx.arcTo(x+w,y,x+w,y+h,r);
    ctx.arcTo(x+w,y+h,x,y+h,r); ctx.arcTo(x,y+h,x,y,r); ctx.arcTo(x,y,x+w,y,r); ctx.closePath(); }

  function drawNode(nd, t){
    const x=nd.x-nd.w/2, y=nd.y-nd.h/2;
    let border=C.dim, glow=null;
    if(nd.kind==='stage'){ border=stageColor(nd.st.status);
      if(nd.st.status!=='healthy') glow=border; }
    if(glow){ const a=0.35+0.35*Math.sin(t*0.006); ctx.save();
      ctx.shadowColor=glow; ctx.shadowBlur=26*a+8; rrect(x,y,nd.w,nd.h,9);
      ctx.strokeStyle=glow; ctx.lineWidth=2.5; ctx.stroke(); ctx.restore(); }
    rrect(x,y,nd.w,nd.h,9); ctx.fillStyle='#111827'; ctx.fill();
    ctx.strokeStyle=border; ctx.lineWidth=2; ctx.stroke();
    ctx.fillStyle=C.text; ctx.font='600 13px ui-monospace,monospace'; ctx.textAlign='center'; ctx.textBaseline='middle';
    ctx.fillText(nd.label, nd.x, nd.y-2);
    if(nd.kind==='stage' && nd.st.badges && nd.st.badges.length){
      ctx.fillStyle=stageColor(nd.st.status); ctx.font='10px ui-monospace,monospace';
      nd.st.badges.slice(0,2).forEach((b,k)=> ctx.fillText(b, nd.x, nd.y+nd.h/2+12+k*12));
    }
  }

  function drawPipe(){
    for(let i=0;i<nodes.length-1;i++){
      const a=nodes[i], b=nodes[i+1];
      ctx.strokeStyle='#1f2937'; ctx.lineWidth=8; ctx.lineCap='round';
      ctx.beginPath(); ctx.moveTo(a.x,a.y); ctx.lineTo(b.x,b.y); ctx.stroke();
    }
  }

  function drawTapLines(){
    stageIdx.forEach((ni,si)=>{
      const nd=nodes[ni]; const dx=detect.x, dy=detect.y-detect.h/2;
      [['data',-10],['ops',10]].forEach(([stream,off])=>{
        ctx.strokeStyle='#1b2430'; ctx.lineWidth=2;
        ctx.beginPath(); ctx.moveTo(nd.x, nd.y+nd.h/2);
        ctx.quadraticCurveTo(nd.x, (nd.y+dy)/2, dx+off, dy); ctx.stroke();
      });
    });
    // stream labels
    ctx.fillStyle=C.sub; ctx.font='11px ui-sans-serif'; ctx.textAlign='left';
    ctx.fillText('observability taps  ·  data metrics + operational signals', 72, H*0.55);
  }

  function tapPos(si, stream, t){
    const nd=nodes[stageIdx[si]]; const off = stream==='data'?-10:10;
    const x0=nd.x, y0=nd.y+nd.h/2, x1=detect.x+off, y1=detect.y-detect.h/2, cx=nd.x, cy=(y0+y1)/2;
    const u=1-t; return {x:u*u*x0+2*u*t*cx+t*t*x1, y:u*u*y0+2*u*t*cy+t*t*y1};
  }

  function drawCorrelation(t){
    STAGES.forEach((s,si)=>{
      if(!s.correlated_from) return;
      const from = nodes[stageIdx[STAGES.findIndex(z=>z.id===s.correlated_from)]];
      const to = nodes[stageIdx[si]];
      if(!from||!to) return;
      ctx.save(); ctx.strokeStyle=C.red; ctx.lineWidth=2.2; ctx.setLineDash([7,6]);
      ctx.lineDashOffset = -(t*0.06)%13;
      ctx.beginPath(); ctx.moveTo(from.x, from.y-from.h/2);
      ctx.quadraticCurveTo((from.x+to.x)/2, from.y-70, to.x, to.y-to.h/2); ctx.stroke();
      ctx.setLineDash([]); ctx.fillStyle=C.red; ctx.font='10px ui-sans-serif'; ctx.textAlign='center';
      ctx.fillText('caused', (from.x+to.x)/2, from.y-74); ctx.restore();
    });
  }

  function dot(x,y,col,r){ ctx.beginPath(); ctx.arc(x,y,r,0,7); ctx.fillStyle=col;
    ctx.shadowColor=col; ctx.shadowBlur=10; ctx.fill(); ctx.shadowBlur=0; }

  function frame(t){
    ctx.clearRect(0,0,W,H);
    drawPipe(); drawTapLines();

    // detection node — pulses with worst severity
    const worst = STAGES.reduce((m,s)=>Math.max(m,sev(s.status)),0);
    const dcol = worst?colorForRank(worst):C.dim;
    const dx=detect.x-detect.w/2, dy=detect.y-detect.h/2;
    if(worst){ const a=0.4+0.4*Math.sin(t*0.008); ctx.save(); ctx.shadowColor=dcol; ctx.shadowBlur=24*a+8;
      rrect(dx,dy,detect.w,detect.h,10); ctx.strokeStyle=dcol; ctx.lineWidth=2.5; ctx.stroke(); ctx.restore(); }
    rrect(dx,dy,detect.w,detect.h,10); ctx.fillStyle='#111827'; ctx.fill();
    ctx.strokeStyle=worst?dcol:'#475569'; ctx.lineWidth=2; ctx.stroke();
    ctx.fillStyle=C.text; ctx.font='600 13px ui-monospace,monospace'; ctx.textAlign='center'; ctx.textBaseline='middle';
    ctx.fillText('DETECTION  ▾', detect.x, detect.y);

    // main pipe particles (colour taints downstream of a fault)
    main.forEach(pt=>{ pt.p=(pt.p+pt.v)%1; const q=pathPoint(pt.p);
      const col = colorForRank(worstUpTo(q.seg)); dot(q.x,q.y,col,3); });

    // tap particles (data=cyan/amber, ops=violet/red)
    taps.forEach(tp=>{ tp.t=(tp.t+tp.v)%1; const s=STAGES[tp.si]; const pos=tapPos(tp.si,tp.stream,tp.t);
      let col = tp.stream==='data' ? (s.status==='data_error'?C.amber:C.flow)
                                   : (s.status==='pipeline_error'?C.red:C.ops);
      dot(pos.x,pos.y,col,2.4); });

    drawCorrelation(t);
    nodes.forEach(nd=>drawNode(nd,t));
    requestAnimationFrame(frame);
  }

  resize(); requestAnimationFrame(frame);
})();
</script>
</body></html>
"""
