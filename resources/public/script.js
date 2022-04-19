function h(tag, attrs, children) {
  if (Array.isArray(attrs)) {
    children = attrs;
    attrs = null;
  }

  var el = document.createElement(tag);
  if (attrs) {
    Object.entries(attrs).forEach(([k, v]) => {
      if (k == 'target') {
        v[0][v[1]] = el;
      } else {
        el.setAttribute(k, v);
      }
    });
  }

  if (children) {
    for (var c of children) {
      el.append(c);
    }
  }

  return el;
}


/// drag

var _dragCounter = 0;

function prevent(e) { e.preventDefault(); }
function dragStart(e) {
  e.target.closest('.dropzone').classList.add('highlight');
  _dragCounter += 1;
}
function dragStop(e)  {
  _dragCounter -= 1;
  if (_dragCounter <= 0) {
    e.target.closest('.dropzone').classList.remove('highlight');
  }
}

function drop(e) {
  _dragCounter = 0;
  e.target.closest('.dropzone').classList.remove('highlight');

  var files = e.dataTransfer.files;

  for (var i = 0; i < files.length; i++) {
    uploadFile(files[i]);
  }
}


/// upload

function elapsedStart(el) {
  var start = +new Date;
  el.timer = setInterval(() => {
    el.innerText = Math.round(((+new Date) - start) / 1000) + 's';
  }, 100);
}

function elapsedStop(el) {
  el.timer && clearInterval(el.timer);
  el.timer = null;
}

function uploadFile(file) {
  var list = document.getElementById('files');
  var t = {};

  list.appendChild(
    h('li', [file.name,
             h('br'),
             h('progress', {target: [t, 'progress']}),
             h('span', {target: [t, 'details']})]));

  var form = new FormData();
  form.append('file', file);

  var xhr = new XMLHttpRequest();
  xhr.upload.addEventListener('progress', (e) => {
    t.progress.max = e.total;
    t.progress.value = e.loaded;
    if (e.total == e.loaded) {
      t.details.setAttribute('aria-busy', 'true');
      elapsedStart(t.details);
    }
  });
  xhr.onreadystatechange = function(e) {
    if (xhr.readyState == 4) {
      t.progress.value = t.progress.max;
      t.details.removeAttribute('aria-busy');
      elapsedStop(t.details);
      t.details.className = (xhr.status == 200 ? "success" : "error");
      t.details.innerText = xhr.responseText;
    }
  };
  xhr.open('POST', 'upload', true);
  xhr.send(form);
}
