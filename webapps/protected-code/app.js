// Access the parameters that end-users filled in using webapp config
// For example, for a parameter called "input_dataset"
// input_dataset = dataiku.getWebAppConfig()['input_dataset']

/*
 * For more information, refer to the "Javascript API" documentation:
 * https://doc.dataiku.com/dss/latest/api/js/index.html
 */


let code_mirror = void 0;

$(function () {
    // code_mirror= CodeMirror(document.getElementById('code-mirror'), {
    //   value: "import cosmian\n",
    //   mode: "python"
    //});
    code_mirror = CodeMirror.fromTextArea(document.getElementById('python-code'), {
        value: "import cosmian\n",
        mode: "python",
        lineNumers: true
    });
    $(".CodeMirror").addClass('grey-border');
    // $('.CodeMirror').resizable({
    //   resize: function() {
    //       code_mirror.setSize($(this).width(), $(this).height());
    //       code_mirror.refresh();
    //   }
    // });
    $('#local_server_url').html(dataiku.getWebAppConfig()['local_server_url']);
    $('#remote_server_url').html(dataiku.getWebAppConfig()['remote_server_url']);
    displayMessage();
});


let deployButton = document.getElementById('deploy-button');

deployButton.addEventListener('click', function (event) {
    let algoNameEl = document.getElementById('algo-name');
    let algoNameValue = algoNameEl.value || '';
    // let pythonCodeEl = document.getElementById('python-code');
    // let pythonCodeValue = pythonCodeEl.value || '';
    let pythonCodeValue = code_mirror.getValue();
    
    event.preventDefault();
    displayMessage('wait', 'deploying code...');

    $.ajax({
        url: getWebAppBackendUrl('/deploy_code'),
        method: 'POST',
        contentType: 'application/json',
        dataType: 'json',
        processData: false,
        data: JSON.stringify({
            local_server_url: dataiku.getWebAppConfig()['local_server_url'],
            remote_server_url: dataiku.getWebAppConfig()['remote_server_url'],
            algo_name: algoNameValue,
            python_code: pythonCodeValue
        })
    }).done(data => {
        console.log(data);
        if (data['status'] === 'ok') {
            displayMessage('success', data['msg']);
        } else {
            displayMessage('error', data['msg']);
        }
    }).fail(err => {
        displayMessage('error', 'ERROR: ' + err.responseText)
    }).then(_ => {
    })
});

function displayMessage(type, msg) {
    let deployResultEl = $("#deploy-result");
    if (type === 'wait') {
        $("#deploy-button").addClass('button-disable');
        $(".spinner").removeClass('hide');
        deployResultEl.html(msg || '');
        deployResultEl.css('color', 'blue');
    } else if (type === 'error') {
        $("#deploy-button").removeClass('button-disable');
        $(".spinner").addClass('hide');
        deployResultEl.html(msg || '');
        deployResultEl.css('color', 'red');
    } else if (type === 'success') {
        $("#deploy-button").removeClass('button-disable');
        $(".spinner").addClass('hide');
        deployResultEl.html(msg || '');
        deployResultEl.css('color', 'green');
    } else {
        $("#deploy-button").removeClass('button-disable');
        $(".spinner").addClass('hide');
        deployResultEl.html(msg || '');
        deployResultEl.css('color', 'black');
    }
}

