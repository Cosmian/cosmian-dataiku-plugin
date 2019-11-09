
let VIEWS=['view-1','view-2','view-3','view-4','view-5','view-6'];



$(function () {
    let list = $("views-list");
    VIEWS.forEach((v, index) => {
        list.append($('<li>').attr("data",index).text(v));
    })
})();
