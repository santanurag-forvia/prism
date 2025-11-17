document.addEventListener('DOMContentLoaded', function () {
        const sidebar = document.getElementById('feasSidebar');
        const sidebarToggle = document.querySelector('.sidebar-toggle-btn');
        const mobileToggle = document.querySelector('.mobile-toggle-btn');
        const overlay = document.getElementById('sidebarOverlay');
        const mainMenu = document.getElementById('mainMenu');
        const userToggle = document.getElementById('userToggle');
        const userMenu = document.getElementById('userMenu');

        // ============================================
        // Sidebar Collapse Toggle (Desktop)
        // ============================================
        if (sidebarToggle) {
          sidebarToggle.addEventListener('click', function () {
            sidebar.classList.toggle('collapsed');
          });
        }

        // ============================================
        // Mobile Sidebar Toggle
        // ============================================
        if (mobileToggle) {
          mobileToggle.addEventListener('click', function () {
            sidebar.classList.toggle('open');
            overlay.classList.toggle('active');
          });
        }

        if (overlay) {
          overlay.addEventListener('click', function () {
            sidebar.classList.remove('open');
            overlay.classList.remove('active');
          });
        }

        // ============================================
        // Submenu Toggle (Accordion - One Open at a Time)
        // ============================================
        if (mainMenu) {
          mainMenu.querySelectorAll('.has-sub-toggle').forEach(function (link) {
            link.addEventListener('click', function (e) {
              e.preventDefault();
              e.stopPropagation();

              const submenu = link.nextElementSibling;
              const isOpen = link.getAttribute('aria-expanded') === 'true';

              // Close all other submenus (accordion behavior)
              mainMenu.querySelectorAll('.submenu').forEach(function (s) {
                if (s !== submenu) {
                  s.classList.remove('open');
                  s.setAttribute('aria-hidden', 'true');
                  s.style.maxHeight = '0';
                }
              });

              mainMenu.querySelectorAll('.has-sub-toggle').forEach(function (l) {
                if (l !== link) {
                  l.classList.remove('active');
                  l.setAttribute('aria-expanded', 'false');
                }
              });

              // Toggle current submenu
              if (!isOpen && submenu && submenu.classList.contains('submenu')) {
                // Open this submenu
                submenu.classList.add('open');
                submenu.setAttribute('aria-hidden', 'false');
                submenu.style.maxHeight = submenu.scrollHeight + 'px';
                link.classList.add('active');
                link.setAttribute('aria-expanded', 'true');
              } else if (submenu && submenu.classList.contains('submenu')) {
                // Close this submenu
                submenu.classList.remove('open');
                submenu.setAttribute('aria-hidden', 'true');
                submenu.style.maxHeight = '0';
                link.classList.remove('active');
                link.setAttribute('aria-expanded', 'false');
              }
            });
          });
        }

        // ============================================
        // User Dropdown Toggle
        // ============================================
        if (userToggle && userMenu) {
          userToggle.addEventListener('click', function (e) {
            e.stopPropagation();
            const show = userMenu.classList.toggle('show');
            userToggle.setAttribute('aria-expanded', show ? 'true' : 'false');
            userMenu.setAttribute('aria-hidden', show ? 'false' : 'true');
          });

          // Close dropdown when clicking outside
          document.addEventListener('click', function (e) {
            if (!userToggle.contains(e.target) && !userMenu.contains(e.target)) {
              userMenu.classList.remove('show');
              userToggle.setAttribute('aria-expanded', 'false');
              userMenu.setAttribute('aria-hidden', 'true');
            }
          });
        }
      });