// Access the parameters that end-users filled in using webapp config
// For example, for a parameter called "input_dataset"
// input_dataset = dataiku.getWebAppConfig()['input_dataset']

/*
 * For more information, refer to the "Javascript API" documentation:
 * https://doc.dataiku.com/dss/latest/api/js/index.html
 */


$(function () {
    displayMessage();
});


let deployButton = document.getElementById('deploy-button');

deployButton.addEventListener('click', function (event) {
    let hostnameEL = document.getElementById('remote-hostname');
    let hostnameValue = hostnameEL.value || '';
    let algoNameEl = document.getElementById('algo-name');
    let algoNameValue = algoNameEl.value || '';
    let pythonCodeEl = document.getElementById('python-code');
    let pythonCodeValue = pythonCodeEl.value || '';
    event.preventDefault();
    displayMessage('wait', 'deploying code...');

    $.ajax({
        url: getWebAppBackendUrl('/deploy_code'),
        method: 'POST',
        contentType: 'application/json',
        dataType: 'json',
        processData: false,
        data: JSON.stringify({
            server_url: dataiku.getWebAppConfig()['server_url'],
            hostname: hostnameValue,
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

