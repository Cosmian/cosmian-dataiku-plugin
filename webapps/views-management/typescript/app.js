let VIEWS = ['view-1', 'view-2', 'view-3', 'view-4', 'view-5', 'view-6'];
// VIEWS=[];

let __selected_view = -1;

$(() => {
    let list = $(".views-list");
    VIEWS.forEach((v, index) => {
        let li = $('<li>').attr("data", index).text(v);
        li.click((ev) => {
            select_view(li, index);
        });
        li.dblclick(() => {
            select_view(li, index);
            edit_view(index);
        });
        list.append(li);
    });
    if (VIEWS.length > 0) {
        select_view($("li[data='0']"), 0);
    }
    $("#btn-delete-view").click(() => handle_delete_view());
});

function load_views() {

}

function deselect_all() {
    $(".views-list li").each((_, li) => {
        $(li).removeClass('views-list-selected')
    });
}

function select_view(li, index) {
    deselect_all();
    li.addClass('views-list-selected');
    $("#btn-delete-view").prop("disabled", false);
    $("#btn-edit-view").prop("disabled", false);
    __selected_view = index;
}

function edit_view(index) {
    console.log("Editing view: " + index);
}

function display_modal(type, message, callback) {
    $(".modal-container").css("display", "flex");
    $(".modal-content p").text(message);
    let btn_other = $("#btn-modal-other");
    let btn_default = $("#btn-modal-default");
    switch (type) {
        case "ok-cancel":
            btn_other.show().text("cancel").click(() => hide_modal(callback, false));
            btn_default.text("ok").click(() => hide_modal(callback, true));
            break;
        case "yes-no":
            btn_other.show().text("no").click(() => hide_modal(callback, false));
            btn_default.text("yes").click(() => hide_modal(callback, true));
            break;
        default:
            btn_other.hide();
            btn_default.text("ok").click(() => hide_modal(callback, true));
    }
}

function hide_modal(callback, response) {
    $(".modal-container").css("display", "none");
    callback(response);
}

function handle_delete_view() {
    let view_name = $("li[data='" + __selected_view + "']");
    if (typeof view_name === "undefined") {
        return
    }
    let msg = "Are you sure you want to delete view '"+view_name+"'";
    display_modal("yes-no", msg, (yes) => {
        if (yes) {
            console.log("PERFORM VIEW DELETE REST OPERATION")
        }
    })

}