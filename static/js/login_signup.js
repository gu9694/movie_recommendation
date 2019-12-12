// 点击评分
$(".kps").on("click", "i", e => {
    let val = $(e.target).index() + 1; // 当前评分的值
    Array.from($('.kps i')).map((v, i) => $(v)[i < val ? 'addClass' : 'removeClass']('mui-icon-star-filled'))
})
  
// 默认点亮3颗星
$('.kps i').eq(3).trigger('click')
