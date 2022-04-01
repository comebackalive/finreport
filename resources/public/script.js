function h(tag, attrs, children) {
  if (Array.isArray(attrs)) {
    children = attrs;
    attrs = null;
  }

  var el = document.createElement(tag);
  if (attrs) {
    Object.entries(attrs).forEach(([k, v]) => {
      el.setAttribute(k, v);
    });
  }

  if (children) {
    for (var c of children) {
      el.append(c);
    }
  }

  return el;
}

function handleDragOver(e) {
  e.preventDefault();
}

function handleDragEnter(e) {
  e.target.classList.add('highlight');
}

function handleDragLeave(e) {
  e.target.classList.remove('highlight');
}

function handleDrop(e) {
  e.preventDefault();
  e.target.classList.remove('highlight');

  var files = e.dataTransfer.files;

  for (var i = 0; i < files.length; i++) {
    uploadFile(files[i]);
  }
}

function uploadFile(file) {
  var list = document.getElementById('files');
  var progress = h('progress');
  var li = h('li', [file.name, h('br'), progress])
  list.appendChild(li);

  var form = new FormData();
  form.append('file', file);

  var xhr = new XMLHttpRequest();
  xhr.upload.addEventListener('progress', (e) => {
    progress.max = e.total;
    progress.value = e.loaded;
  });
  xhr.onreadystatechange = function(e) {
    if (xhr.readyState == 4) {
      progress.value = progress.max;
      progress.className = (xhr.status == 200 ? "success" : "failure");
      li.appendChild(h('span', [xhr.responseText]));
    }
  };
  xhr.open('POST', 'upload', true);
  xhr.send(form);
}
