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
});

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
    console.log("Editing view: "+index);
}
