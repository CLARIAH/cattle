
rum = {}; 
indent = 2;

$(document).ready(function(){


	$('#left').click(function(){
		var jsondata = editor.getValue();
		if(jsondata != ""){
			rum = JSON.parse(jsondata);
		}
		updateForm();
		localStorage.cowjson = jsondata;
		return false;
	});

	$('#right').click(function(){
		updateJson();
		return false;
	});

	$('#save').click(function(){
		console.log("you want to save this?!");
		saveJSON();
	});

	$("#left img").hover(function(){
		$(this).attr('src','../../static/ruminator/img/cow-left.png');
	},
	function(){
		$(this).attr('src','../../static/ruminator/img/cow-left-light.png');	
	});

	$("#right img").hover(function(){
		$(this).attr('src','../../static/ruminator/img/cow-right.png');
	},
	function(){
		$(this).attr('src','../../static/ruminator/img/cow-right-light.png');	
	});

	$('#to-dataset').click(function(){
		showTab('#dataset');
	});

	$('#to-columns').click(function(){
		showTab('#columncontainer');
	});

	$('#to-help').click(function(){
		showTab('#help');
	});

	$('#to-context').click(function(){
		showTab('#context');
	});

	$('#expand').click(function(){
		$('.columncontent').show();
		return false;
	});

	$('#collapse').click(function(){
		$('.columncontent').hide();
		return false;
	});


	showTab('#columns');

	var cowjsontextarea = document.getElementById('cowjson');
	editor = CodeMirror.fromTextArea(cowjsontextarea, {
		matchBrackets: true,
		autoCloseBrackets: true,
		mode: "application/ld+json",
		lineWrapping: false,
		lineNumbers: true
	  });

	// // see if there's any data stored
	// if (localStorage.cowjson) {
	// 	editor.setValue(localStorage.cowjson);
	// } else {
	// 	console.log('no cowjson found');
	// 	getJSON();
	// }

	receiveJSON();
	// getJSON();

});

//old version
// function receiveJSON() {
// 	console.log("handling file at location: " + fileLocation);
// 	$.getJSON(fileLocation, function(json) {
// 		jsonFile = JSON.stringify(json, null, indent);
// 		editor.setValue(jsonFile);
// 	});
// }

// function receiveJSON() {
// 	console.log("handling file at location: " + fileLocation);
// 	$.get("/get_current_json", function(json) {
//     	// console.log($.parseJSON(json));
//     	jsonFile = JSON.stringify(json, null, indent);
// 		editor.setValue(jsonFile);

// 	})
// }
function receiveJSON() {
	jsonContents = jsonContents.substring(1, jsonContents.length-1);
	editor.setValue(jsonContents);
}

// function getJSON() {
// 	console.log("getting json");
// 	var json_location = getUrlParam('x','testdata/testdata-1.json');
// 	// index.html?x=probeersel/imf.csv-metadata.json
// 	$.getJSON(json_location, function(json) {
// 	// $.getJSON('testdata/testdata-1.json', function(json) {
// 		testjson = JSON.stringify(json, null, indent);
// 		editor.setValue(testjson);
// 	});
// }

function saveJSON() {
	$.post( "/save_json", {
		javascript_data: editor.getValue()
	});
}

// function getUrlVars() {
// 	var vars = {};
// 	var parts = window.location.href.replace(/[?&]+([^=&]+)=([^&]*)/gi, function(m,key,value) {
// 		vars[key] = value;
// 	});
// 	return vars;
// }

// function getUrlParam(parameter, defaultvalue){
// 	var urlparameter = defaultvalue;
// 	if(window.location.href.indexOf(parameter) > -1){
// 		urlparameter = getUrlVars()[parameter];
// 		}
// 	return urlparameter;
// }


function showTab(tab){
	$('.tabcontent').hide();
	$(tab).show();
	$('.tabs button').removeClass('current');
	if(tab=="#columncontainer"){
		$('.tabs #to-columns').addClass('current');
	}else{
		$('.tabs #to-'+tab.replace('#','')).addClass('current');
	}
}

function updateForm(){

	// create columns
	$('#columns').empty();
	var cols = rum.tableSchema.columns;
	$.each(cols, function(index){

		columnBlock = createColumnBlock(this, index);
		columnBlock.appendTo('#columns');
		
	});

	// populate dataset metadata (dialect, publisher, license)
	$('#dataset').empty();
	metadataBlock = createMetadataBlock(rum);
	metadataBlock.appendTo('#dataset');

	// bind some actions to freshly made elements
	bindevents();

	// and show columns
	showTab('#columncontainer');
}	


function bindevents(){
	$('.column h3').click(function(){
		$(this).siblings('.columncontent').toggle();
	});

	$('.columncontent select[name="datatype"]').change(function(){
		var datatype = $(this).val();
		if(datatype=="xsd:string"){
			$(this).siblings('input[name="lang"]').show();
		}else{
			$(this).siblings('input[name="lang"]').hide();
			$(this).siblings('input[name="lang"]').val('');
		}
	});

	$('.addvirtual').click(function(){
		var regcolumn = $(this).closest('.regular-column');
		var column = $('#vir-col-template .cc').clone();
		column.addClass('column virtual-column').removeClass('cc');
		column.children('.columncontent').css('display','block');
		column.children('h3').click(function(){
			$(this).siblings('.columncontent').toggle();
		});
		column.children('.columncontent').children('select[name="datatype"]').change(function(){
			var datatype = $(this).val();
			if(datatype=="string"){
				$(this).siblings('input[name="lang"]').show();
			}else{
				$(this).siblings('input[name="lang"]').hide();
				$(this).siblings('input[name="lang"]').val('');
			}
		});
		column.children('.columncontent').children('input[name="csvw:value"]').hide();
		column.children('.columncontent').children('select[name="objecttype"]').change(changeObjectType);
		column.children('.columncontent').children('.delvirtual').click(function(){
			var vircolumn = $(this).closest('.virtual-column');
			vircolumn.remove();
			return false;
		});
		column.insertAfter(regcolumn);
		return false;
	});

	$('.delvirtual').click(function(){
		var vircolumn = $(this).closest('.virtual-column');
		vircolumn.remove();
		return false;
	});
	
	$('select[name="objecttype"]').change(changeObjectType);
	
}


function changeObjectType(){
	var objecttype = $(this).val();
	if(objecttype=="valueUrl"){
		$(this).siblings('input[name="csvw:value"]').hide();
		$(this).siblings('input[name="csvw:value"]').val('');
		$(this).siblings('input[name="valueUrl"]').show();
	}
	if(objecttype=="csvw:value"){
		$(this).siblings('input[name="valueUrl"]').hide();
		$(this).siblings('input[name="valueUrl"]').val('');
		$(this).siblings('input[name="csvw:value"]').show();
	}
}


function updateJson(){

	// columns
	var cols = [];
	$('.column').each(function(){
		var content = $(this).children('.columncontent');
		var originalJSON = content.children('input[name="jsondata"]').val();
		var thiscol = JSON.parse(originalJSON);

		content.children('.attribute').each(function(index){
			var attrvalue = $(this).val();
			if(attrvalue==''){
				delete thiscol[this.name];
			}else{
				thiscol[this.name] = $(this).val();
			}
		});
		
		cols.push(thiscol);
	});
	rum.tableSchema.columns = cols;


	// dataset metadata (dialect, publisher, license)
	rum.dialect.quoteChar = $('#quoteChar').val();
	rum.dialect.delimiter = $('#delimiter').val();
	rum.dialect.encoding = $('#encoding').val();
	rum['dc:publisher']['schema:name'] = $('#publisher-name').val();
	rum['dc:publisher']['schema:url']['@id'] = $('#publisher-url').val();

	var prettified = JSON.stringify(rum, null, indent);
	var topline = editor.getScrollInfo();
	//console.log(topline);
	editor.setValue(prettified);
		
	editor.scrollTo(topline.left,topline.top);

	localStorage.cowjson = prettified;

}



function formatJSON(input, indent) {
	if (input.length == 0) {
		return '';
	}
	else {
		var parsedData = JSON.parse(input);
		return JSON.stringify(parsedData, null, indent);
	}
}

function createColumnBlock(coldata, index){
	if(coldata.virtual==true){
		var column = $('#vir-col-template .cc').clone();
		column.addClass('column virtual-column').removeClass('cc');
		var content = column.children('.columncontent');
		if ( typeof coldata['csvw:value'] !== 'undefined'){
			content.children("select[name=objecttype]").val('csvw:value');
			content.children('input[name="valueUrl"]').hide();
		}else{
			content.children("select[name=objecttype]").val('valueUrl');
			content.children('input[name="csvw:value"]').hide();
		}
	}else{
		var column = $('#reg-col-template .cc').clone();
		column.addClass('column regular-column').removeClass('cc');
		var content = column.children('.columncontent');
		column.children('h3').html(coldata.name);
	}
	content.children('input[name="jsondata"]').val(JSON.stringify(coldata));
	content.children('textarea[name="dc:description"]').html(coldata['dc:description']);
	if ( typeof coldata['datatype'] !== 'undefined'){
		if(coldata['datatype'].startsWith('xsd:')==false){ // should be prefixed with xsd:
			coldata['datatype'] = 'xsd:' + coldata['datatype'];
		}
		var exists = false;
		content.children('select[name="datatype"]').children('option').each(function(){
			if (this.value == coldata['datatype']) {
				exists = true;
				return false;
			}
		});
		if(exists===false){ // accept all datatypes from json
			content.children('select[name="datatype"]').append($("<option></option>")
					.attr("value",coldata['datatype'])
					.text(coldata['datatype'])); 
		}
		content.children('select[name="datatype"]').val(coldata['datatype']);
		if(coldata['datatype']=='xsd:string'){
			content.children('input[name="lang"]').css('display','block');
		}
	}
	$.each(coldata, function(colname){
		content.children('input[name="'+colname+'"]').val(coldata[colname]);
	});

	return column;
}


function createMetadataBlock(cowdata){
	
	var block = $('<div/>', {
		class: 'metadata'
	});
	$('<h3/>', {
			text: 'dialect'
	}).appendTo(block);
	$('<input/>', {
		type: 'text',
		value: cowdata['dialect']['quoteChar'],
		class: 'form-control small',
		id: 'quoteChar',
		placeholder: 'quote char'
	}).appendTo(block);
	$('<input/>', {
		type: 'text',
		value: cowdata['dialect']['delimiter'],
		class: 'form-control small',
		id: 'delimiter'
	}).appendTo(block);
	$('<input/>', {
		type: 'text',
		value: cowdata['dialect']['encoding'],
		class: 'form-control small',
		id: 'encoding'
	}).appendTo(block);
	$('<h3/>', {
			text: 'publisher'
	}).appendTo(block);
	$('<input/>', {
		type: 'text',
		value: cowdata['dc:publisher']['schema:name'],
		class: 'form-control',
		id: 'publisher-name'
	}).appendTo(block);
	$('<input/>', {
		type: 'text',
		value: cowdata['dc:publisher']['schema:url']['@id'],
		class: 'form-control',
		id: 'publisher-url'
	}).appendTo(block);
	$('<h3/>', {
			text: 'license'
	}).appendTo(block);
	$('<input/>', {
		type: 'text',
		value: cowdata['dc:license']['@id'],
		class: 'form-control',
		id: 'license'
	}).appendTo(block);
	
	return block;
}

