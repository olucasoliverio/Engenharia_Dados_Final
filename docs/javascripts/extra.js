// Animação "reveal": elementos com a classe .reveal aparecem ao entrar na tela.
// Compatível com a navegação instantânea do Material (document$.subscribe).
(function () {
  function ativarReveal() {
    var alvos = document.querySelectorAll(".reveal");
    if (!alvos.length) return;

    if (!("IntersectionObserver" in window)) {
      alvos.forEach(function (el) { el.classList.add("is-visible"); });
      return;
    }

    var obs = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) {
            entry.target.classList.add("is-visible");
            obs.unobserve(entry.target);
          }
        });
      },
      { threshold: 0.12 }
    );

    alvos.forEach(function (el) { obs.observe(el); });
  }

  if (window.document$ && typeof window.document$.subscribe === "function") {
    window.document$.subscribe(ativarReveal);
  } else {
    document.addEventListener("DOMContentLoaded", ativarReveal);
  }
})();
