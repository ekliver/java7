/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sys.bean;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import javax.faces.application.FacesMessage;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ManagedProperty;
import javax.faces.bean.ViewScoped;
import javax.faces.context.FacesContext;
import javax.servlet.ServletContext;
import org.primefaces.context.RequestContext;
import sys.clasesAuxiliares.Funciones;
import sys.clasesAuxiliares.reportesNotaDespacho;
import sys.dao.FacturaDAO;
import sys.dao.FacturaDetalleDAO;
import sys.dao.MovimientoAlmacenDetalleDAO;
import sys.dao.NotaDespachoDAO;
import sys.dao.NotaDespachoDetalleDAO;
import sys.imp.FacturaDAOImp;
import sys.imp.FacturaDetalleDAOImp;
import sys.imp.MovimientoAlmacenDetalleDAOImp;
import sys.imp.NotaDespachoDAOImp;
import sys.imp.NotaDespachoDetalleDAOImp;
import sys.model.AimarMovAlmacenDet;
import sys.model.AvmovFacturaNdCab;
import sys.model.AvmovFacturaNdDet;
import sys.model.AvmovMovNotaDespachoCab;
import sys.model.AvmovMovNotaDespachoDet;

@ManagedBean
@ViewScoped
public class FacturaBean implements Serializable {

    @ManagedProperty("#{loginBean}")
    private LoginBean lBean;

    private AvmovMovNotaDespachoCab notaDespacho;
    private List<AvmovMovNotaDespachoCab> listaNotaDespachoPendientes;

    private AvmovMovNotaDespachoDet notaDespachoDetalle;
    private List<AvmovMovNotaDespachoDet> listaNotaDespachoDetalle;

    private Date feDesde;
    private Date feHasta;
    private Date feMaxima;

    //Variables de factura
    private AvmovFacturaNdCab factura;
    private List<AvmovFacturaNdCab> listaFactura;
    private AvmovFacturaNdDet facturaDetalle;
    private List<AvmovFacturaNdDet> listaFacturaDetalle;
    private List<AvmovFacturaNdDet> listaProductosPendientes;
    private List<AvmovFacturaNdDet> seleccionProducto;

    //variable de edicion
    private boolean esNuevo;
    private List<AvmovFacturaNdDet> itemsEliminado;
    private String nameBtnSave;
    private String nameFocus;
    private boolean estadoNumeroSerie;

    private String filtroNroND;

    public FacturaBean() {

        Calendar calendar = Calendar.getInstance();
        feDesde = new Date();
        feHasta = new Date();
        feMaxima = new Date();
        calendar.add(Calendar.MONTH, -1);
        feDesde = calendar.getTime();

//        notaDespacho = new AvmovMovNotaDespachoCab();
//        listaNotaDespachoPendientes = new ArrayList<>();

        FacturaDAO fDao = new FacturaDAOImp();

        factura = new AvmovFacturaNdCab();
        listaFactura = new ArrayList<>();
//        facturaDetalle = new AvmovFacturaNdDet();
//        listaFacturaDetalle = new ArrayList<>();

        listaFactura = fDao.listarFacturasPorFecha(feDesde, feHasta);

    }
// <editor-fold desc="GETTERS AND SETTER">

    public LoginBean getlBean() {
        return lBean;
    }

    public void setlBean(LoginBean lBean) {
        this.lBean = lBean;
    }

    public AvmovMovNotaDespachoCab getNotaDespacho() {
        return notaDespacho;
    }

    public void setNotaDespacho(AvmovMovNotaDespachoCab notaDespacho) {
        this.notaDespacho = notaDespacho;
    }

    public List<AvmovMovNotaDespachoCab> getListaNotaDespachoPendientes() {
        return listaNotaDespachoPendientes;
    }

    public void setListaNotaDespachoPendientes(List<AvmovMovNotaDespachoCab> listaNotaDespachoPendientes) {
        this.listaNotaDespachoPendientes = listaNotaDespachoPendientes;
    }

    public AvmovMovNotaDespachoDet getNotaDespachoDetalle() {
        return notaDespachoDetalle;
    }

    public void setNotaDespachoDetalle(AvmovMovNotaDespachoDet notaDespachoDetalle) {
        this.notaDespachoDetalle = notaDespachoDetalle;
    }

    public List<AvmovMovNotaDespachoDet> getListaNotaDespachoDetalle() {
        return listaNotaDespachoDetalle;
    }

    public void setListaNotaDespachoDetalle(List<AvmovMovNotaDespachoDet> listaNotaDespachoDetalle) {
        this.listaNotaDespachoDetalle = listaNotaDespachoDetalle;
    }

    public Date getFeDesde() {
        return feDesde;
    }

    public void setFeDesde(Date feDesde) {
        this.feDesde = feDesde;
    }

    public Date getFeHasta() {
        return feHasta;
    }

    public void setFeHasta(Date feHasta) {
        this.feHasta = feHasta;
    }

    public Date getFeMaxima() {
        return feMaxima;
    }

    public void setFeMaxima(Date feMaxima) {
        this.feMaxima = feMaxima;
    }

    public AvmovFacturaNdCab getFactura() {
        return factura;
    }

    public void setFactura(AvmovFacturaNdCab factura) {
        this.factura = factura;
    }

    public List<AvmovFacturaNdCab> getListaFactura() {
        return listaFactura;
    }

    public void setListaFactura(List<AvmovFacturaNdCab> listaFactura) {
        this.listaFactura = listaFactura;
    }

    public AvmovFacturaNdDet getFacturaDetalle() {
        return facturaDetalle;
    }

    public void setFacturaDetalle(AvmovFacturaNdDet facturaDetalle) {
        this.facturaDetalle = facturaDetalle;
    }

    public List<AvmovFacturaNdDet> getListaFacturaDetalle() {
        return listaFacturaDetalle;
    }

    public void setListaFacturaDetalle(List<AvmovFacturaNdDet> listaFacturaDetalle) {
        this.listaFacturaDetalle = listaFacturaDetalle;
    }

    public List<AvmovFacturaNdDet> getItemsEliminado() {
        return itemsEliminado;
    }

    public void setItemsEliminado(List<AvmovFacturaNdDet> itemsEliminado) {
        this.itemsEliminado = itemsEliminado;
    }

    public List<AvmovFacturaNdDet> getListaProductosPendientes() {
        return listaProductosPendientes;
    }

    public void setListaProductosPendientes(List<AvmovFacturaNdDet> listaProductosPendientes) {
        this.listaProductosPendientes = listaProductosPendientes;
    }

    public List<AvmovFacturaNdDet> getSeleccionProducto() {
        return seleccionProducto;
    }

    public void setSeleccionProducto(List<AvmovFacturaNdDet> seleccionProducto) {
        this.seleccionProducto = seleccionProducto;
    }

    public String getNameBtnSave() {
        return nameBtnSave;
    }

    public void setNameBtnSave(String nameBtnSave) {
        this.nameBtnSave = nameBtnSave;
    }

    public String getNameFocus() {
        return nameFocus;
    }

    public void setNameFocus(String nameFocus) {
        this.nameFocus = nameFocus;
    }

    public boolean isEstadoNumeroSerie() {
        return estadoNumeroSerie;
    }

    public void setEstadoNumeroSerie(boolean estadoNumeroSerie) {
        this.estadoNumeroSerie = estadoNumeroSerie;
    }

    public boolean isEsNuevo() {
        return esNuevo;
    }

    public void setEsNuevo(boolean esNuevo) {
        this.esNuevo = esNuevo;
    }

    public String getFiltroNroND() {
        return filtroNroND;
    }

    public void setFiltroNroND(String filtroNroND) {
        this.filtroNroND = filtroNroND;
    }

// </editor-fold>
    public void listarNDPendientes() {
        NotaDespachoDAO nDDao = new NotaDespachoDAOImp();
        listaNotaDespachoPendientes = nDDao.listarNotaDespachosAFactura();

    }

    public void consultarPorFechas() {
        FacturaDAO fDao = new FacturaDAOImp();
        listaFactura = fDao.listarFacturasPorFecha(feDesde, feHasta);

    }

    public void generarNroFactura() {
        estadoNumeroSerie = true;
        FacturaDAO fDao = new FacturaDAOImp();
        factura.setNumFactura(fDao.generarNroFactura(factura));

    }

    public void prepararNuevaFactura(AvmovMovNotaDespachoCab nd) {
        estadoNumeroSerie = false;
        esNuevo = true;
        nameBtnSave = "Guardar";
        nameFocus = "numSerie";
        filtroNroND = "";
//iniciando Variables de Factura
        factura = new AvmovFacturaNdCab();
        facturaDetalle = new AvmovFacturaNdDet();
        notaDespacho = new AvmovMovNotaDespachoCab();
        notaDespacho = nd;
        FacturaDAO fDao = new FacturaDAOImp();
        factura = fDao.obtenerFacturaPorND(nd);
        factura.setNumSerie("");
        factura.setNumFactura("");
        factura.setFecFactura(new Date());
        factura.setFlgTipoFactura("S");
        factura.setNumAnyio(lBean.getAnyo());
        factura.setFlgPrioridad("1");
        factura.setFlgEstado("P");
        factura.setCodMoneda("1");
        factura.setCodTipoDocumento("01");

        factura.setNumImporteSubtotal(factura.getZvalorSubTotalSol());
        factura.setNumImporteIgv(factura.getZvalorIgvSol());
        factura.setNumImporteTotal(factura.getZvalorTotalSol());
        FacturaDetalleDAO fDDao = new FacturaDetalleDAOImp();
        listaFacturaDetalle = fDDao.obtenerListaFacturaDetallePorND(notaDespacho);

        Funciones fun = new Funciones();
        if (factura.getFlgTipoFactura().equals("S")) {
            factura.setDesImporteTotal(fun.Numeros_Letras(factura.getNumImporteTotal()) + " SOLES");
        } else {
            factura.setDesImporteTotal(fun.Numeros_Letras(factura.getNumImporteTotal()) + " DOLARES");
        }
        itemsEliminado = new ArrayList();
    }

    public void prepararEditarFactura(AvmovFacturaNdCab f) {
        estadoNumeroSerie = true;
        esNuevo = false;
        nameBtnSave = "Actualizar";
        nameFocus = "tablaFacturaDetalle";
//iniciando Variables de Factura
        factura = new AvmovFacturaNdCab();
        facturaDetalle = new AvmovFacturaNdDet();

        FacturaDAO fDao = new FacturaDAOImp();
        factura = fDao.obtenerFacturaPorIdFactura(f);

        FacturaDetalleDAO fDDao = new FacturaDetalleDAOImp();
        listaFacturaDetalle = fDDao.obtenerListaFacturaDetallePorIdFactura(factura);
        itemsEliminado = new ArrayList();
    }

    public void cambioTipoFactura() {
        double subTotal = 0;
        if (factura.getFlgTipoFactura().equals("S")) {
            for (AvmovFacturaNdDet item : listaFacturaDetalle) {
                if (item.getCodMonedaOrigen().equals("1")) {
                    item.setNumPrecioUnitario(item.getNumPrecioOrigen());
                } else {
                    item.setNumPrecioUnitario((double) Math.round((item.getNumPrecioOrigen() * factura.getValDolar()) * 100) / 100);
                }
                item.setNumImporteProducto(item.getNumPrecioUnitario() * item.getNumCntdProducto());
                subTotal += item.getNumImporteProducto();
                item.setEditado(true);
            }

            factura.setCodMoneda("1");

        } else {
            for (AvmovFacturaNdDet item : listaFacturaDetalle) {
                if (item.getCodMonedaOrigen().equals("2")) {
                    item.setNumPrecioUnitario(item.getNumPrecioOrigen());
                } else {
                    item.setNumPrecioUnitario((double) Math.round((item.getNumPrecioOrigen() / factura.getValDolar()) * 100) / 100);
                }
                item.setNumImporteProducto(item.getNumPrecioUnitario() * item.getNumCntdProducto());
                subTotal += item.getNumImporteProducto();
                item.setEditado(true);
            }

            factura.setCodMoneda("2");
        }

        factura.setNumImporteSubtotal(subTotal);
        factura.setNumImporteIgv(subTotal * factura.getValIgv() / 100);
        factura.setNumImporteTotal(factura.getNumImporteSubtotal() + factura.getNumImporteIgv());

        Funciones fun = new Funciones();
        if (factura.getFlgTipoFactura().equals("S")) {
            factura.setDesImporteTotal(fun.Numeros_Letras(factura.getNumImporteTotal()) + " SOLES");
        } else {
            factura.setDesImporteTotal(fun.Numeros_Letras(factura.getNumImporteTotal()) + " DOLARES");
        }

    }

    public void prepararAgregarProducto() {
        listaProductosPendientes = new ArrayList<>();
        FacturaDetalleDAO fDDao = new FacturaDetalleDAOImp();
        listaProductosPendientes = fDDao.obtenerListaProductoPendientesPorIdRel(notaDespacho, listaFacturaDetalle);
    }

    public void agregarFacturaDetalle() {
        for (AvmovFacturaNdDet item : seleccionProducto) {
            listaFacturaDetalle.add(item);
        }
        cambioTipoFactura();

    }

    public void quitarFacturaDetalle(AvmovFacturaNdDet fd) {
        if (fd.isNuevo() == false) {
            itemsEliminado.add(fd);
        }

        listaFacturaDetalle.remove(fd);
        calcularTotal();
    }

    public void calcularTotal() {
        double subTotal = 0;
        for (AvmovFacturaNdDet item : listaFacturaDetalle) {
            subTotal += item.getNumImporteProducto();
        }
        factura.setNumImporteSubtotal(subTotal);
        factura.setNumImporteIgv(subTotal * factura.getValIgv() / 100);
        factura.setNumImporteTotal(factura.getNumImporteSubtotal() + factura.getNumImporteIgv());
        Funciones fun = new Funciones();
        if (factura.getFlgTipoFactura().equals("S")) {
            factura.setDesImporteTotal(fun.Numeros_Letras(factura.getNumImporteTotal()) + " SOLES");
        } else {
            factura.setDesImporteTotal(fun.Numeros_Letras(factura.getNumImporteTotal()) + " DOLARES");
        }

    }

    public void guardarFactura() {
        FacturaDAO fDao = new FacturaDAOImp();
        FacturaDetalleDAO fDDao = new FacturaDetalleDAOImp();
        MovimientoAlmacenDetalleDAO mADDao = new MovimientoAlmacenDetalleDAOImp();
        AimarMovAlmacenDet mAD = new AimarMovAlmacenDet();

        NotaDespachoDetalleDAO nDDDao = new NotaDespachoDetalleDAOImp();

        if (esNuevo) {
            if (factura.getNumFactura().length() > 4) {
                if (fDao.obtenerIdFacturaPorNumFactura(factura) == 0) {

                    
                        factura.setCodUsuarioCreacion(lBean.getUsuario().getCodUsuario());
                        fDao.newFactura(factura);

                        factura.setIdFacturaCab(fDao.obtenerIdFacturaPorNumFactura(factura));
                        for (AvmovFacturaNdDet item : listaFacturaDetalle) {
                            item.setIdFacturaCab(factura.getIdFacturaCab());
                            item.setNumFactura(factura.getNumFactura());
                            item.setCodUsuarioCreacion(lBean.getUsuario().getCodUsuario());
                            fDDao.newFacturaDetalle(item);
                            mAD = new AimarMovAlmacenDet();
                            mAD.setIdNotaDespachoDet(item.getIdNotaDespachoDet());
                            mAD.setValVtaProd(item.getNumPrecioUnitario());
                            mAD.setImpItem(item.getNumImporteProducto());

                            if (item.getCodMonedaOrigen().equals("1")) {
                                mAD.setValVtaSol(item.getNumPrecioOrigen());
                                mAD.setImpItemSol(mAD.getValVtaSol() * item.getNumCntdProducto());
                                mAD.setValVtaDol(item.getNumPrecioOrigen() * factura.getValDolar());
                                mAD.setImpItemDol(mAD.getValVtaDol() * item.getNumCntdProducto());
                            } else {
                                mAD.setValVtaSol(Math.round((item.getNumPrecioOrigen() / factura.getValDolar()) * 100) / 100);
                                mAD.setImpItemSol(mAD.getValVtaSol() * item.getNumCntdProducto());
                                mAD.setValVtaDol(item.getNumPrecioOrigen());
                                mAD.setImpItemDol(mAD.getValVtaDol() * item.getNumCntdProducto());

                            }

                            mAD.setCodUsuarioActualizacion(item.getCodUsuarioCreacion());
                            mADDao.updatePrecioMovimientoAlmacenDetalle(mAD);

                            nDDDao.updateFactutacionNotaDespachoDetalle(item.getIdNotaDespachoDet(), 1);

                        }

                        listaFactura = fDao.listarFacturasPorFecha(feDesde, feHasta);
                        listarNDPendientes();
                        filtroNroND = factura.getNumNotaDespacho();
                        FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(
                                FacesMessage.SEVERITY_INFO,
                                "Informacion",
                                "Se guardo con exito la Factura #" + factura.getNumFactura() + "! "));
                        RequestContext.getCurrentInstance().execute("PF('dialogNuevaFactura').hide();PF('dialogSeleccionND').show();PF('wvTablaSeleccionND').filter();");

                    
                } else {
                    FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN, "Informacion", "La Factura #" + factura.getNumFactura() + " ya existe debe cambiarla"));
                }
            } else {
                FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN, "Informacion", "Falta ingresar el numero de la Factura"));
            }
        } else {

            
                factura.setCodUsuarioActualizacion(lBean.getUsuario().getCodUsuario());
                fDao.updateFactura(factura);

                for (AvmovFacturaNdDet item : listaFacturaDetalle) {

                    if (item.isNuevo()) {
                        item.setIdFacturaCab(factura.getIdFacturaCab());
                        item.setNumFactura(factura.getNumFactura());
                        item.setCodUsuarioCreacion(lBean.getUsuario().getCodUsuario());
                        fDDao.newFacturaDetalle(item);
                    } else {
                        if (item.isEditado()) {
                            fDDao.updateFacturaDetalle(item);
                        }
                    }
                    mAD = new AimarMovAlmacenDet();
                    mAD.setIdNotaDespachoDet(item.getIdNotaDespachoDet());
                    mAD.setValVtaProd(item.getNumPrecioUnitario());
                    mAD.setImpItem(item.getNumImporteProducto());

                    if (item.getCodMonedaOrigen().equals("1")) {
                        mAD.setValVtaSol(item.getNumPrecioOrigen());
                        mAD.setImpItemSol(mAD.getValVtaSol() * item.getNumCntdProducto());
                        mAD.setValVtaDol(item.getNumPrecioOrigen() * factura.getValDolar());
                        mAD.setImpItemDol(mAD.getValVtaDol() * item.getNumCntdProducto());
                    } else {
                        mAD.setValVtaSol(Math.round((item.getNumPrecioOrigen() / factura.getValDolar()) * 100) / 100);
                        mAD.setImpItemSol(mAD.getValVtaSol() * item.getNumCntdProducto());
                        mAD.setValVtaDol(item.getNumPrecioOrigen());
                        mAD.setImpItemDol(mAD.getValVtaDol() * item.getNumCntdProducto());

                    }

                    mAD.setCodUsuarioActualizacion(item.getCodUsuarioCreacion());
                    mADDao.updatePrecioMovimientoAlmacenDetalle(mAD);

                    nDDDao.updateFactutacionNotaDespachoDetalle(item.getIdNotaDespachoDet(), 1);

                }

                for (AvmovFacturaNdDet item : itemsEliminado) {
                    fDDao.deleteFacturaDetalle(item);
                    nDDDao.updateFactutacionNotaDespachoDetalle(item.getIdNotaDespachoDet(), 0);

                    mAD = new AimarMovAlmacenDet();
                    mAD.setIdNotaDespachoDet(item.getIdNotaDespachoDet());
                    mAD.setValVtaProd(0.0);
                    mAD.setImpItem(0.0);
                    mAD.setValVtaSol(0.0);
                    mAD.setImpItemSol(0.0);
                    mAD.setValVtaDol(0.0);
                    mAD.setImpItemDol(0.0);

                    mAD.setCodUsuarioActualizacion(item.getCodUsuarioCreacion());
                    mADDao.updatePrecioMovimientoAlmacenDetalle(mAD);

                }

                listaFactura = fDao.listarFacturasPorFecha(feDesde, feHasta);

                FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_INFO,
                        "Informacion", "Se guardo con exito la Factura #" + factura.getNumFactura() + "! "));
                RequestContext.getCurrentInstance().execute("PF('dialogNuevaFactura').hide();");
            
        }
    }

    public void anularFactura(AvmovFacturaNdCab f) {
        FacturaDAO fDao = new FacturaDAOImp();
        f = fDao.obtenerFacturaPorIdFactura(f);
        f.setFlgEstado("A");
        f.setCodUsuarioActualizacion(lBean.getUsuario().getCodUsuario());
        fDao.updateFactura(f);
        listaFactura = fDao.listarFacturasPorFecha(feDesde, feHasta);
        FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN, "Informacion", "Se anulo la factura #" + f.getNumFactura()));

    }

    public void verReporteFacturaNotaDespacho(AvmovFacturaNdCab f) throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {

        reportesNotaDespacho rFactura = new reportesNotaDespacho();

        FacesContext facesContext = FacesContext.getCurrentInstance();
        ServletContext servletContext = (ServletContext) facesContext.getExternalContext().getContext();
        String ruta = servletContext.getRealPath("/reports/rpt_FacturaNotaDespacho.jasper");

        rFactura.getReporteFactura(ruta, f.getIdFacturaCab());
        FacesContext.getCurrentInstance().responseComplete();

    }

}
