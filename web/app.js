const e = React.createElement;

function App() {
  const [apiKey, setApiKey] = React.useState(localStorage.getItem("storageApiKey") || "dev-secret-key");
  const [buckets, setBuckets] = React.useState([]), [bucket, setBucket] = React.useState("");
  const [objects, setObjects] = React.useState([]), [cluster, setCluster] = React.useState(null);
  const [repairs, setRepairs] = React.useState([]), [message, setMessage] = React.useState("");
  const [newBucket, setNewBucket] = React.useState(""), [prefix, setPrefix] = React.useState("");
  const [file, setFile] = React.useState(null), [objectKey, setObjectKey] = React.useState("");

  const api = async (path, options={}) => {
    const response = await fetch(path, {...options, headers:{"X-API-Key":apiKey, ...(options.headers||{})}});
    if (!response.ok) { let detail=await response.text(); try { detail=JSON.parse(detail).detail; } catch(_){} throw new Error(detail); }
    if (response.status === 204) return null;
    return response.headers.get("content-type")?.includes("json") ? response.json() : response.blob();
  };
  const run = async fn => { try { setMessage(""); await fn(); } catch(err) { setMessage("Error: "+err.message); } };
  const loadBuckets = () => run(async()=>{ const rows=await api("/buckets"); setBuckets(rows); if(!bucket&&rows[0])setBucket(rows[0].bucket||rows[0].name); });
  const loadObjects = () => bucket && run(async()=>setObjects(await api(`/buckets/${bucket}/objects?prefix=${encodeURIComponent(prefix)}`)));
  const loadStatus = () => run(async()=>{ setCluster(await api("/cluster/status")); setRepairs(await api("/cluster/repairs")); });
  React.useEffect(()=>{ localStorage.setItem("storageApiKey",apiKey); },[apiKey]);
  React.useEffect(()=>{ loadBuckets(); loadStatus(); },[]);
  React.useEffect(()=>{ loadObjects(); },[bucket]);

  const createBucket = () => run(async()=>{ await api("/buckets",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({name:newBucket})}); setNewBucket(""); await loadBuckets(); });
  const upload = () => run(async()=>{ if(!file||!bucket)throw new Error("Select a bucket and file"); const key=objectKey||file.name; await api(`/buckets/${bucket}/objects/${encodeURIComponent(key)}`,{method:"PUT",headers:{"X-Object-Tags":"{}"},body:file}); setFile(null);setObjectKey("");setMessage("Upload complete");await loadObjects();await loadStatus(); });
  const download = row => run(async()=>{ const blob=await api(`/buckets/${row.bucket}/objects/${encodeURIComponent(row.key)}?version=${row.version}`); const link=document.createElement("a");link.href=URL.createObjectURL(blob);link.download=row.key.split("/").pop();link.click();URL.revokeObjectURL(link.href); });
  const remove = row => run(async()=>{ await api(`/buckets/${row.bucket}/objects/${encodeURIComponent(row.key)}?version=${row.version}`,{method:"DELETE"});await loadObjects();await loadStatus(); });
  const search = () => run(async()=>setObjects(await api(`/search/objects?bucket=${encodeURIComponent(bucket)}&prefix=${encodeURIComponent(prefix)}`)));

  return e("div",{className:"shell"},
    e("header",null,e("div",null,e("h1",null,"Distributed Object Storage"),e("div",{className:"muted"},"Section 6 operations console")),
      e("div",{className:"auth"},e("input",{value:apiKey,onChange:x=>setApiKey(x.target.value),placeholder:"API key"}),e("button",{onClick:()=>{loadBuckets();loadStatus();}},"Connect"))),
    e("div",{className:"message"},message),
    e("main",{className:"grid"},
      e("section",{className:"panel"},e("h2",null,"Buckets"),e("div",{className:"row"},e("input",{value:newBucket,onChange:x=>setNewBucket(x.target.value),placeholder:"new-bucket"}),e("button",{onClick:createBucket},"+")),
        e("div",{className:"stack",style:{marginTop:12}},...buckets.map(b=>{const name=b.bucket||b.name;return e("button",{key:name,className:"bucket "+(name===bucket?"active":""),onClick:()=>setBucket(name)},name);}))),
      e("section",{className:"panel"},e("h2",null,"Objects · "+(bucket||"select a bucket")),
        e("div",{className:"toolbar"},e("input",{value:prefix,onChange:x=>setPrefix(x.target.value),placeholder:"Search key prefix"}),e("button",{className:"ghost",onClick:loadObjects},"List"),e("button",{onClick:search},"Elasticsearch")),
        e("div",{className:"row",style:{marginBottom:14}},e("input",{type:"file",onChange:x=>setFile(x.target.files[0])}),e("input",{value:objectKey,onChange:x=>setObjectKey(x.target.value),placeholder:"optional object key"}),e("button",{onClick:upload},"Upload")),
        e("table",null,e("thead",null,e("tr",null,e("th",null,"Key"),e("th",null,"Size"),e("th",null,"Version"),e("th",null,"Actions"))),
          e("tbody",null,...objects.map((o,i)=>e("tr",{key:o.key+o.version+i},e("td",null,o.key),e("td",null,o.size.toLocaleString()),e("td",null,e("span",{className:"pill"},"v"+o.version)),e("td",null,e("button",{className:"ghost",onClick:()=>download(o)},"Download")," ",e("button",{className:"danger",onClick:()=>remove(o)},"Delete"))))))),
      e("aside",{className:"panel"},e("div",{className:"row",style:{justifyContent:"space-between"}},e("h2",null,"Cluster"),e("button",{className:"ghost",onClick:loadStatus},"Refresh")),
        cluster&&e(React.Fragment,null,e("div",{className:"stat"},e("span",null,"Free space"),e("b",null,(cluster.total_free_space_bytes/1073741824).toFixed(1)+" GiB")),e("div",{className:"stat"},e("span",null,"Chunks"),e("b",null,cluster.total_chunks)),e("div",{className:"stat"},e("span",null,"Degraded"),e("b",{className:cluster.degraded_objects.length?"bad":"ok"},cluster.degraded_objects.length)),
          ...cluster.nodes.map(n=>e("div",{className:"node",key:n.node_id},e("b",null,n.node_id),e("span",{className:n.status==="active"?"ok":"bad"},n.status),e("span",{className:"muted"},n.chunk_count+" chunks"),e("span",{className:"muted"},(n.free_space_bytes/1073741824).toFixed(1)+" GiB")))),
        e("h2",{style:{marginTop:18}},"Repair progress"),repairs.length?repairs.slice(0,6).map(r=>e("div",{className:"node",key:r.id},e("span",null,r.strategy+" · "+r.event_type),e("span",{className:r.status==="repaired"?"ok":"bad"},r.status),e("span",{className:"muted"},r.detail),e("span",{className:"muted"},r.bytes_transferred+" B"))):e("div",{className:"muted"},"No repair events"))));
}
ReactDOM.createRoot(document.getElementById("root")).render(e(App));
