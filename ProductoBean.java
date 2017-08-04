/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sys.bean;

import java.io.Serializable;
import java.util.ArrayList;
import sys.imp.ProductoDAOImp;
import java.util.List;
import javax.faces.application.FacesMessage;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ManagedProperty;
import javax.faces.bean.ViewScoped;
import javax.faces.context.FacesContext;
import org.primefaces.event.RowEditEvent;
import sys.dao.ProductoDAO;
import sys.dao.ProductoPrecioDAO;
import sys.imp.ProductoPrecioDAOImp;
import sys.model.AimarProductos;
import sys.model.ZProductoPrecio;

@ManagedBean
@ViewScoped
public class ProductoBean implements Serializable {

    @ManagedProperty("#{loginBean}")
    private LoginBean lBean;

    private List<AimarProductos> listaProductos;
    private AimarProductos producto;

    private List<ZProductoPrecio> listaProductoPrecios;
    private ZProductoPrecio productoPrecio;


    public ProductoBean() {
        producto = new AimarProductos();
        productoPrecio = new ZProductoPrecio();
        listaProductoPrecios = new ArrayList<>();
        

        ProductoPrecioDAO proPreDao = new ProductoPrecioDAOImp();
        listaProductoPrecios = proPreDao.listarProductoPrecios();

    }

    public LoginBean getlBean() {
        return lBean;
    }

    public void setlBean(LoginBean lBean) {
        this.lBean = lBean;
    }

    public List<AimarProductos> getListaProductos() {
        ProductoDAO pDao = new ProductoDAOImp();
        listaProductos = pDao.listarProductos();
        return listaProductos;
    }

    public void setListaProductos(List<AimarProductos> listaProductos) {
        this.listaProductos = listaProductos;
    }

    public AimarProductos getProducto() {
        return producto;
    }

    public void setProducto(AimarProductos producto) {
        this.producto = producto;
    }

    public List<ZProductoPrecio> getListaProductoPrecios() {
        return listaProductoPrecios;
    }

    public void setListaProductoPrecios(List<ZProductoPrecio> listaProductoPrecios) {
        this.listaProductoPrecios = listaProductoPrecios;
    }

    public ZProductoPrecio getProductoPrecio() {
        return productoPrecio;
    }

    public void setProductoPrecio(ZProductoPrecio productoPrecio) {
        this.productoPrecio = productoPrecio;
    }

    public void prepararNuevoProductoPrecio() {
        productoPrecio = new ZProductoPrecio();

    }

    public void nuevoProductoPrecio() {
        ProductoPrecioDAO pPDao = new ProductoPrecioDAOImp();
        productoPrecio.setCodUsuarioCreacion(lBean.getUsuario().getCodUsuario());
        pPDao.newProductoPrecio(productoPrecio);
        listaProductoPrecios= pPDao.listarProductoPrecios();
    }

    public void modificarProductoPrecio() {

        ProductoPrecioDAO pPDao = new ProductoPrecioDAOImp();
        productoPrecio.setCodUsuarioActualizacion(lBean.getUsuario().getCodUsuario());
        pPDao.updateProductoPrecio(productoPrecio);
        listaProductoPrecios = pPDao.listarProductoPrecios();
        productoPrecio = new ZProductoPrecio();
    }

    public void eliminarProductoPrecio() {
        ProductoPrecioDAO pPDao = new ProductoPrecioDAOImp();
        pPDao.deleteProductoPrecio(productoPrecio);
        listaProductoPrecios = pPDao.listarProductoPrecios();
        productoPrecio = new ZProductoPrecio();
    }

    public void onRowEdit(RowEditEvent event) {
        productoPrecio.setCodProducto(String.valueOf(((ZProductoPrecio) event.getObject()).getCodProducto()));
        productoPrecio.setPrecioProducto(Double.valueOf(String.valueOf(((ZProductoPrecio) event.getObject()).getPrecioProducto())));
        modificarProductoPrecio();
        FacesMessage msg = new FacesMessage("Precio editado", String.valueOf(((ZProductoPrecio) event.getObject()).getPrecioProducto()));
        FacesContext.getCurrentInstance().addMessage(null, msg);
    }

    public void onRowCacel(RowEditEvent event) {
        FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_INFO, "Informacion", "No se hizo ningun cambio"));
    }

}
