document.getElementById("drop_zone").ondrop = dropHandler;
document.getElementById("drop_zone").ondragover = dragOverHandler;
document.getElementById("drop_zone").ondragenter = dragEnterHandler;
document.getElementById("drop_zone").ondragleave = dragLeaveHandler;

function dropHandler(ev) {
  console.log('File(s) dropped');

  // Prevent default behavior (Prevent file from being opened)
  ev.preventDefault();

  var files = [];
  if (ev.dataTransfer.items) {
    // Use DataTransferItemList interface to access the file(s)
    for (var i = 0; i < ev.dataTransfer.items.length; i++) {
      if (ev.dataTransfer.items[i].kind === 'file') {
        files.push(ev.dataTransfer.items[i].getAsFile());
      }
    }
  } else {
    // Use DataTransfer interface to access the file(s)
    for (var i = 0; i < ev.dataTransfer.files.length; i++) {
      files.push(ev.dataTransfer.files[i])
    }
  }

  if (files.length > 2) {
    console.log("TOO MANY FILES!!");
    document.getElementById("drop_zone_text").innerHTML = "You can only upload up to 2 files, 1 CSV file and an optional JSON file.";
    document.getElementById("drop_zone_text").style.color = "red";
    document.getElementById("drop_zone").className = "drop_zone";
    return 0;
  };

  var formData = new FormData();
  for (var i = 0; i < files.length; i++) {
    if (files[i].name.endsWith(".csv") || files[i].name.endsWith(".tsv")) {
      console.log("found csv file!");
      formData.append("csv", files[i]);
    } else if (files[i].name.endsWith(".json")) {
      console.log("found json file!");
      formData.append("json", files[i]);
    }
  }

  var req = {
      url: "/build_convert",
      method: "post",
      processData: false,
      contentType: false,
      data: formData,
      success: function(response){
        document.write(response); 
      }
  };
  $.ajax(req);
}

function dragOverHandler(ev) {
  // Prevent default behavior (Prevent file from being opened)
  ev.preventDefault();
  ev.currentTarget.className = "hover_zone";
}

function dragEnterHandler(ev) {
  ev.currentTarget.className = "hover_zone";
}

function dragLeaveHandler(ev) {
  ev.currentTarget.className = "drop_zone";
}